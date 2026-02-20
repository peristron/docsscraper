"""
D2L Documentation Site Scraper - Standalone Validator
Run this separately to audit what's being scraped and validate completeness.
"""

import httpx
from bs4 import BeautifulSoup
from urllib.parse import urljoin, urlparse, urldefrag
import time
import json
from pathlib import Path
from collections import defaultdict
from datetime import datetime
import re

# Configuration
BASE_URL = "https://docs.valence.desire2learn.com/"
MAX_PAGES = None  # None = unlimited, or set a number for testing
CRAWL_DELAY = 0.2
OUTPUT_DIR = Path("scrape_audit")
TIMEOUT = 30.0

# Create output directory
OUTPUT_DIR.mkdir(exist_ok=True)

class SiteAuditor:
    def __init__(self):
        self.visited = set()
        self.pages = []
        self.failed_urls = []
        self.skipped_urls = []
        self.url_map = defaultdict(list)  # Track where URLs were found
        self.route_count = 0
        self.categories = defaultdict(int)
        
        self.client = httpx.Client(
            timeout=TIMEOUT,
            follow_redirects=True,
            headers={"User-Agent": "D2L-API-Auditor/1.0"}
        )
    
    def normalize_url(self, url: str) -> str:
        """Normalize URL for comparison"""
        url, _ = urldefrag(url)
        if url.endswith("/index.html"):
            url = url[:-10]
        return url.rstrip("/")
    
    def is_valid(self, url: str) -> bool:
        """Check if URL should be crawled"""
        parsed = urlparse(url)
        
        # Must be same domain
        if parsed.hostname != "docs.valence.desire2learn.com":
            return False
        
        # Skip non-HTML files
        skip_ext = {".png", ".jpg", ".gif", ".css", ".js", ".zip", ".pdf", ".txt", ".svg", ".ico"}
        if any(parsed.path.lower().endswith(ext) for ext in skip_ext):
            return False
        
        # Skip common non-content paths
        skip_paths = ["/_static/", "/_sources/", "/genindex.html", "/search.html"]
        if any(skip in parsed.path for skip in skip_paths):
            return False
        
        return True
    
    def extract_api_routes(self, content: str) -> list:
        """Extract API routes from content"""
        pattern = re.compile(r"(GET|POST|PUT|PATCH|DELETE)\s+(/d2l/api/[\w/{}().~\-]+)", re.IGNORECASE)
        matches = pattern.findall(content)
        return [(method.upper(), path) for method, path in matches]
    
    def crawl_page(self, url: str, parent_url: str = None):
        """Crawl a single page and extract information"""
        try:
            print(f"Crawling: {url}")
            
            response = self.client.get(url)
            
            if response.status_code != 200:
                self.failed_urls.append({
                    "url": url,
                    "status": response.status_code,
                    "parent": parent_url
                })
                print(f"  ❌ Status {response.status_code}")
                return None, []
            
            if "text/html" not in response.headers.get("content-type", ""):
                self.skipped_urls.append({
                    "url": url,
                    "reason": "Not HTML",
                    "content_type": response.headers.get("content-type")
                })
                print(f"  ⏭️  Skipped (not HTML)")
                return None, []
            
            soup = BeautifulSoup(response.text, "html.parser")
            
            # Extract title
            title = soup.find("title")
            title = title.get_text(strip=True) if title else "Untitled"
            
            # Find main content
            main = (
                soup.find("div", {"role": "main"}) or
                soup.find("div", class_="document") or
                soup.find("div", class_="rst-content") or
                soup.find("article") or
                soup.find("main") or
                soup.find("body")
            )
            
            if not main:
                self.skipped_urls.append({
                    "url": url,
                    "reason": "No main content found",
                    "title": title
                })
                print(f"  ⚠️  No main content")
                return None, []
            
            content = main.get_text(separator="\n", strip=True)
            
            # Extract category from URL path
            path_parts = [p for p in urlparse(url).path.split("/") if p]
            category = path_parts[0] if path_parts else "root"
            if category.endswith(".html"):
                category = "root"
            
            self.categories[category] += 1
            
            # Extract API routes
            routes = self.extract_api_routes(content)
            self.route_count += len(routes)
            
            # Build page data
            page_data = {
                "url": url,
                "title": title,
                "content_length": len(content),
                "word_count": len(content.split()),
                "category": category,
                "routes_found": len(routes),
                "routes": routes,
                "parent_url": parent_url,
                "crawled_at": datetime.now().isoformat()
            }
            
            print(f"  ✅ {len(content)} chars, {len(routes)} routes, category: {category}")
            
            # Extract links
            links = []
            for a in soup.find_all("a", href=True):
                href = a["href"]
                
                # Skip anchors and mailto
                if href.startswith("#") or href.startswith("mailto:"):
                    continue
                
                # Make absolute
                abs_url = urljoin(url, href)
                norm_url = self.normalize_url(abs_url)
                
                # Track where we found this link
                self.url_map[norm_url].append(url)
                
                if self.is_valid(norm_url) and norm_url not in self.visited:
                    links.append(norm_url)
            
            return page_data, links
        
        except httpx.TimeoutException:
            self.failed_urls.append({
                "url": url,
                "error": "Timeout",
                "parent": parent_url
            })
            print(f"  ❌ Timeout")
            return None, []
        
        except Exception as e:
            self.failed_urls.append({
                "url": url,
                "error": str(e),
                "parent": parent_url
            })
            print(f"  ❌ Error: {e}")
            return None, []
    
    def crawl_all(self):
        """Crawl entire site"""
        print(f"\n{'='*60}")
        print(f"Starting crawl of {BASE_URL}")
        print(f"{'='*60}\n")
        
        start_time = time.time()
        
        queue = [self.normalize_url(BASE_URL)]
        self.visited.add(queue[0])
        
        while queue:
            if MAX_PAGES and len(self.pages) >= MAX_PAGES:
                print(f"\n⚠️  Reached MAX_PAGES limit ({MAX_PAGES})")
                break
            
            url = queue.pop(0)
            parent = self.url_map[url][0] if self.url_map[url] else None
            
            page_data, links = self.crawl_page(url, parent)
            
            if page_data and page_data["content_length"] > 100:
                self.pages.append(page_data)
            
            # Add new links to queue
            for link in links:
                if link not in self.visited:
                    self.visited.add(link)
                    queue.append(link)
            
            # Rate limiting
            time.sleep(CRAWL_DELAY)
            
            # Progress update
            if len(self.pages) % 10 == 0:
                print(f"\n📊 Progress: {len(self.pages)} pages, {len(queue)} in queue\n")
        
        elapsed = time.time() - start_time
        
        print(f"\n{'='*60}")
        print(f"Crawl Complete!")
        print(f"{'='*60}")
        print(f"Time: {elapsed:.1f}s")
        print(f"Pages: {len(self.pages)}")
        print(f"Failed: {len(self.failed_urls)}")
        print(f"Skipped: {len(self.skipped_urls)}")
        print(f"API Routes: {self.route_count}")
        print(f"{'='*60}\n")
    
    def generate_reports(self):
        """Generate comprehensive reports"""
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        
        # 1. Summary Report
        summary = {
            "crawl_timestamp": timestamp,
            "base_url": BASE_URL,
            "total_pages": len(self.pages),
            "total_visited_urls": len(self.visited),
            "failed_urls": len(self.failed_urls),
            "skipped_urls": len(self.skipped_urls),
            "total_api_routes": self.route_count,
            "categories": dict(self.categories),
            "pages_by_category": dict(self.categories)
        }
        
        summary_file = OUTPUT_DIR / f"summary_{timestamp}.json"
        summary_file.write_text(json.dumps(summary, indent=2))
        print(f"✅ Summary: {summary_file}")
        
        # 2. Full Page List
        pages_file = OUTPUT_DIR / f"pages_{timestamp}.json"
        pages_file.write_text(json.dumps(self.pages, indent=2))
        print(f"✅ Pages: {pages_file}")
        
        # 3. Failed URLs
        if self.failed_urls:
            failed_file = OUTPUT_DIR / f"failed_{timestamp}.json"
            failed_file.write_text(json.dumps(self.failed_urls, indent=2))
            print(f"⚠️  Failed: {failed_file}")
        
        # 4. URL Map (for debugging broken links)
        urlmap_file = OUTPUT_DIR / f"urlmap_{timestamp}.json"
        urlmap_data = {url: parents for url, parents in self.url_map.items()}
        urlmap_file.write_text(json.dumps(urlmap_data, indent=2))
        print(f"✅ URL Map: {urlmap_file}")
        
        # 5. Routes Report
        all_routes = []
        for page in self.pages:
            for method, path in page.get("routes", []):
                all_routes.append({
                    "method": method,
                    "path": path,
                    "found_on": page["url"],
                    "page_title": page["title"]
                })
        
        routes_file = OUTPUT_DIR / f"routes_{timestamp}.json"
        routes_file.write_text(json.dumps(all_routes, indent=2))
        print(f"✅ Routes: {routes_file} ({len(all_routes)} routes)")
        
        # 6. Human-readable text report
        report_lines = [
            "="*70,
            "D2L Documentation Scrape Report",
            "="*70,
            f"Generated: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}",
            f"Base URL: {BASE_URL}",
            "",
            "SUMMARY",
            "-"*70,
            f"Total Pages Scraped: {len(self.pages)}",
            f"Total URLs Visited: {len(self.visited)}",
            f"Failed URLs: {len(self.failed_urls)}",
            f"Skipped URLs: {len(self.skipped_urls)}",
            f"API Routes Found: {self.route_count}",
            "",
            "CATEGORIES",
            "-"*70
        ]
        
        for category, count in sorted(self.categories.items(), key=lambda x: -x[1]):
            report_lines.append(f"  {category:30s} {count:5d} pages")
        
        report_lines.extend([
            "",
            "TOP 20 PAGES BY CONTENT",
            "-"*70
        ])
        
        sorted_pages = sorted(self.pages, key=lambda x: x["content_length"], reverse=True)[:20]
        for i, page in enumerate(sorted_pages, 1):
            report_lines.append(f"{i:2d}. {page['title'][:50]:50s} ({page['content_length']:,} chars)")
            report_lines.append(f"    {page['url']}")
        
        if self.failed_urls:
            report_lines.extend([
                "",
                "FAILED URLS",
                "-"*70
            ])
            for fail in self.failed_urls[:20]:
                report_lines.append(f"  {fail.get('status', 'Error')}: {fail['url']}")
                if fail.get('error'):
                    report_lines.append(f"    Error: {fail['error']}")
        
        report_lines.append("="*70)
        
        report_file = OUTPUT_DIR / f"report_{timestamp}.txt"
        report_file.write_text("\n".join(report_lines))
        print(f"✅ Report: {report_file}")
        
        # 7. Create comparison file for your app
        comparison = {
            "expected_minimum_pages": len(self.pages),
            "expected_minimum_routes": self.route_count,
            "categories": list(self.categories.keys()),
            "sample_urls": [p["url"] for p in self.pages[:50]],
            "validation_timestamp": timestamp
        }
        
        comparison_file = OUTPUT_DIR / "expected_coverage.json"
        comparison_file.write_text(json.dumps(comparison, indent=2))
        print(f"✅ Validation: {comparison_file}")
        
        return summary
    
    def close(self):
        self.client.close()


def compare_with_app_scrape(app_metadata_file: str = "scrape_metadata.json"):
    """Compare this audit with what your app scraped"""
    
    print("\n" + "="*60)
    print("COMPARISON WITH APP SCRAPE")
    print("="*60 + "\n")
    
    # Load expected coverage
    expected_file = OUTPUT_DIR / "expected_coverage.json"
    if not expected_file.exists():
        print("❌ No expected_coverage.json found. Run audit first.")
        return
    
    expected = json.loads(expected_file.read_text())
    
    # Load app's scrape metadata
    app_file = Path(app_metadata_file)
    if not app_file.exists():
        print(f"❌ App metadata file not found: {app_metadata_file}")
        return
    
    app_data = json.loads(app_file.read_text())
    
    # Compare
    print("📊 Coverage Comparison:\n")
    
    expected_pages = expected["expected_minimum_pages"]
    actual_pages = app_data.get("pages_count", 0)
    
    coverage_pct = (actual_pages / expected_pages * 100) if expected_pages > 0 else 0
    
    print(f"Pages:")
    print(f"  Expected (audit): {expected_pages}")
    print(f"  Actual (app):     {actual_pages}")
    print(f"  Coverage:         {coverage_pct:.1f}%")
    
    if coverage_pct >= 95:
        print(f"  Status:           ✅ EXCELLENT")
    elif coverage_pct >= 80:
        print(f"  Status:           ⚠️  GOOD (but could be better)")
    else:
        print(f"  Status:           ❌ INCOMPLETE")
    
    print(f"\nRoutes:")
    expected_routes = expected["expected_minimum_routes"]
    print(f"  Expected (audit): {expected_routes}")
    print(f"  Note: App doesn't track total routes in metadata")
    
    print(f"\nChunks:")
    print(f"  App created:      {app_data.get('chunks_count', 0):,}")
    print(f"  Vectors stored:   {app_data.get('vectors_count', 0):,}")
    
    print("\n" + "="*60)


if __name__ == "__main__":
    import sys
    
    print("""
    ╔════════════════════════════════════════════════════════════╗
    ║     D2L Documentation Scraper - Validation Tool            ║
    ║                                                            ║
    ║  This will crawl the entire D2L docs site and generate    ║
    ║  detailed reports for comparison with your main app.      ║
    ╚════════════════════════════════════════════════════════════╝
    """)
    
    print("Options:")
    print("  1. Run full site audit (recommended)")
    print("  2. Run quick audit (first 50 pages)")
    print("  3. Compare with app scrape")
    print("  4. Exit")
    
    choice = input("\nEnter choice (1-4): ").strip()
    
    if choice == "1":
        MAX_PAGES = None
        print("\n🚀 Starting FULL audit (this may take 10-15 minutes)...\n")
        
        auditor = SiteAuditor()
        try:
            auditor.crawl_all()
            summary = auditor.generate_reports()
            
            print("\n✅ Audit complete! Check the 'scrape_audit' folder for reports.\n")
            
            # Automatically compare if app metadata exists
            if Path("scrape_metadata.json").exists():
                compare_with_app_scrape()
            
        finally:
            auditor.close()
    
    elif choice == "2":
        MAX_PAGES = 50
        print("\n🚀 Starting QUICK audit (50 pages)...\n")
        
        auditor = SiteAuditor()
        try:
            auditor.crawl_all()
            auditor.generate_reports()
            print("\n✅ Quick audit complete!\n")
        finally:
            auditor.close()
    
    elif choice == "3":
        compare_with_app_scrape()
    
    elif choice == "4":
        print("\n👋 Goodbye!\n")
        sys.exit(0)
    
    else:
        print("\n❌ Invalid choice\n")
        sys.exit(1)
