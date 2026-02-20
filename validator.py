"""
D2L Documentation Site Scraper - Streamlit Validator
Standalone tool to audit and validate documentation scraping.
"""

import streamlit as st
import httpx
from bs4 import BeautifulSoup
from urllib.parse import urljoin, urlparse, urldefrag
import time
import json
from pathlib import Path
from collections import defaultdict
from datetime import datetime
import re
import pandas as pd
import plotly.express as px
import plotly.graph_objects as go

# Page config
st.set_page_config(
    page_title="D2L Docs Scraper Validator",
    page_icon="🔍",
    layout="wide"
)

# Configuration
BASE_URL = "https://docs.valence.desire2learn.com/"
OUTPUT_DIR = Path("scrape_audit")
TIMEOUT = 30.0

# Create output directory
OUTPUT_DIR.mkdir(exist_ok=True)

# Initialize session state
if "audit_results" not in st.session_state:
    st.session_state.audit_results = None
if "crawl_running" not in st.session_state:
    st.session_state.crawl_running = False

# Title and intro
st.title("🔍 D2L Documentation Scraper Validator")
st.markdown("""
This tool crawls the entire D2L Valence API documentation site and generates
detailed reports to validate your main app's scraping coverage.
""")

# Sidebar configuration
with st.sidebar:
    st.header("⚙️ Configuration")
    
    crawl_delay = st.slider(
        "Crawl Delay (seconds)",
        min_value=0.0,
        max_value=2.0,
        value=0.2,
        step=0.1,
        help="Delay between requests to be respectful to the server"
    )
    
    max_pages = st.number_input(
        "Max Pages (0 = unlimited)",
        min_value=0,
        max_value=1000,
        value=0,
        help="Limit for testing. Set to 0 for full site audit."
    )
    
    max_pages = None if max_pages == 0 else max_pages
    
    st.divider()
    
    st.header("📊 Quick Stats")
    if st.session_state.audit_results:
        results = st.session_state.audit_results
        st.metric("Pages Scraped", results["total_pages"])
        st.metric("API Routes Found", results["total_routes"])
        st.metric("Categories", len(results["categories"]))
        st.metric("Failed URLs", results["failed_count"])
    else:
        st.info("Run an audit to see stats")


class SiteAuditor:
    def __init__(self, progress_callback=None, status_callback=None):
        self.visited = set()
        self.pages = []
        self.failed_urls = []
        self.skipped_urls = []
        self.url_map = defaultdict(list)
        self.route_count = 0
        self.categories = defaultdict(int)
        self.progress_callback = progress_callback
        self.status_callback = status_callback
        
        self.client = httpx.Client(
            timeout=TIMEOUT,
            follow_redirects=True,
            headers={"User-Agent": "D2L-API-Auditor-Streamlit/1.0"}
        )
    
    def log(self, message):
        """Log message via callback"""
        if self.status_callback:
            self.status_callback(message)
    
    def update_progress(self, current, total, url=""):
        """Update progress via callback"""
        if self.progress_callback:
            self.progress_callback(current, total, url)
    
    def normalize_url(self, url: str) -> str:
        url, _ = urldefrag(url)
        if url.endswith("/index.html"):
            url = url[:-10]
        return url.rstrip("/")
    
    def is_valid(self, url: str) -> bool:
        parsed = urlparse(url)
        
        if parsed.hostname != "docs.valence.desire2learn.com":
            return False
        
        skip_ext = {".png", ".jpg", ".gif", ".css", ".js", ".zip", ".pdf", ".txt", ".svg", ".ico"}
        if any(parsed.path.lower().endswith(ext) for ext in skip_ext):
            return False
        
        skip_paths = ["/_static/", "/_sources/", "/genindex.html", "/search.html"]
        if any(skip in parsed.path for skip in skip_paths):
            return False
        
        return True
    
    def extract_api_routes(self, content: str) -> list:
        pattern = re.compile(r"(GET|POST|PUT|PATCH|DELETE)\s+(/d2l/api/[\w/{}().~\-]+)", re.IGNORECASE)
        matches = pattern.findall(content)
        return [(method.upper(), path) for method, path in matches]
    
    def crawl_page(self, url: str, parent_url: str = None):
        try:
            response = self.client.get(url)
            
            if response.status_code != 200:
                self.failed_urls.append({
                    "url": url,
                    "status": response.status_code,
                    "parent": parent_url
                })
                return None, []
            
            if "text/html" not in response.headers.get("content-type", ""):
                self.skipped_urls.append({
                    "url": url,
                    "reason": "Not HTML",
                    "content_type": response.headers.get("content-type")
                })
                return None, []
            
            soup = BeautifulSoup(response.text, "html.parser")
            
            title = soup.find("title")
            title = title.get_text(strip=True) if title else "Untitled"
            
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
                return None, []
            
            content = main.get_text(separator="\n", strip=True)
            
            path_parts = [p for p in urlparse(url).path.split("/") if p]
            category = path_parts[0] if path_parts else "root"
            if category.endswith(".html"):
                category = "root"
            
            self.categories[category] += 1
            
            routes = self.extract_api_routes(content)
            self.route_count += len(routes)
            
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
            
            links = []
            for a in soup.find_all("a", href=True):
                href = a["href"]
                
                if href.startswith("#") or href.startswith("mailto:"):
                    continue
                
                abs_url = urljoin(url, href)
                norm_url = self.normalize_url(abs_url)
                
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
            return None, []
        
        except Exception as e:
            self.failed_urls.append({
                "url": url,
                "error": str(e),
                "parent": parent_url
            })
            return None, []
    
    def crawl_all(self, max_pages=None, crawl_delay=0.2):
        start_time = time.time()
        
        queue = [self.normalize_url(BASE_URL)]
        self.visited.add(queue[0])
        
        total_estimate = max_pages if max_pages else 300
        
        while queue:
            if max_pages and len(self.pages) >= max_pages:
                self.log(f"⚠️ Reached max pages limit ({max_pages})")
                break
            
            url = queue.pop(0)
            parent = self.url_map[url][0] if self.url_map[url] else None
            
            # Update progress
            self.update_progress(len(self.pages) + 1, total_estimate, url)
            
            page_data, links = self.crawl_page(url, parent)
            
            if page_data and page_data["content_length"] > 100:
                self.pages.append(page_data)
            
            for link in links:
                if link not in self.visited:
                    self.visited.add(link)
                    queue.append(link)
            
            time.sleep(crawl_delay)
        
        elapsed = time.time() - start_time
        
        return {
            "total_pages": len(self.pages),
            "total_visited": len(self.visited),
            "failed_count": len(self.failed_urls),
            "skipped_count": len(self.skipped_urls),
            "total_routes": self.route_count,
            "categories": dict(self.categories),
            "elapsed_time": elapsed,
            "pages": self.pages,
            "failed_urls": self.failed_urls,
            "skipped_urls": self.skipped_urls,
            "url_map": dict(self.url_map)
        }
    
    def close(self):
        self.client.close()


def save_results(results):
    """Save audit results to files"""
    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    
    # Summary
    summary = {
        "crawl_timestamp": timestamp,
        "base_url": BASE_URL,
        "total_pages": results["total_pages"],
        "total_routes": results["total_routes"],
        "categories": results["categories"],
        "elapsed_time": results["elapsed_time"]
    }
    
    summary_file = OUTPUT_DIR / f"summary_{timestamp}.json"
    summary_file.write_text(json.dumps(summary, indent=2))
    
    # Full pages
    pages_file = OUTPUT_DIR / f"pages_{timestamp}.json"
    pages_file.write_text(json.dumps(results["pages"], indent=2))
    
    # Routes
    all_routes = []
    for page in results["pages"]:
        for method, path in page.get("routes", []):
            all_routes.append({
                "method": method,
                "path": path,
                "found_on": page["url"],
                "page_title": page["title"]
            })
    
    routes_file = OUTPUT_DIR / f"routes_{timestamp}.json"
    routes_file.write_text(json.dumps(all_routes, indent=2))
    
    # Expected coverage
    comparison = {
        "expected_minimum_pages": results["total_pages"],
        "expected_minimum_routes": results["total_routes"],
        "categories": list(results["categories"].keys()),
        "sample_urls": [p["url"] for p in results["pages"][:50]],
        "validation_timestamp": timestamp
    }
    
    comparison_file = OUTPUT_DIR / "expected_coverage.json"
    comparison_file.write_text(json.dumps(comparison, indent=2))
    
    return {
        "summary": summary_file,
        "pages": pages_file,
        "routes": routes_file,
        "comparison": comparison_file
    }


def display_results(results):
    """Display results in Streamlit"""
    
    st.success(f"✅ Audit Complete! Scraped {results['total_pages']} pages in {results['elapsed_time']:.1f}s")
    
    # Key metrics
    col1, col2, col3, col4 = st.columns(4)
    col1.metric("Pages Scraped", results["total_pages"])
    col2.metric("API Routes", results["total_routes"])
    col3.metric("Categories", len(results["categories"]))
    col4.metric("Failed URLs", results["failed_count"])
    
    # Tabs for different views
    tab1, tab2, tab3, tab4, tab5 = st.tabs([
        "📊 Overview", 
        "📄 Pages", 
        "🔗 Routes", 
        "⚠️ Issues",
        "💾 Export"
    ])
    
    with tab1:
        st.header("Overview")
        
        # Category breakdown
        col_a, col_b = st.columns(2)
        
        with col_a:
            st.subheader("Pages by Category")
            category_df = pd.DataFrame([
                {"Category": cat, "Pages": count}
                for cat, count in results["categories"].items()
            ]).sort_values("Pages", ascending=False)
            
            fig = px.bar(
                category_df,
                x="Category",
                y="Pages",
                title="Distribution of Pages by Category"
            )
            st.plotly_chart(fig, use_container_width=True)
        
        with col_b:
            st.subheader("Content Size Distribution")
            sizes = [p["content_length"] for p in results["pages"]]
            
            fig = px.histogram(
                x=sizes,
                nbins=30,
                title="Page Content Length Distribution",
                labels={"x": "Content Length (chars)", "y": "Count"}
            )
            st.plotly_chart(fig, use_container_width=True)
        
        # Top pages
        st.subheader("Top 10 Pages by Content Size")
        top_pages = sorted(results["pages"], key=lambda x: x["content_length"], reverse=True)[:10]
        
        top_df = pd.DataFrame([
            {
                "Title": p["title"][:50],
                "Category": p["category"],
                "Size (chars)": f"{p['content_length']:,}",
                "Routes": p["routes_found"],
                "URL": p["url"]
            }
            for p in top_pages
        ])
        
        st.dataframe(top_df, use_container_width=True, hide_index=True)
    
    with tab2:
        st.header("All Scraped Pages")
        
        # Filters
        col_f1, col_f2 = st.columns(2)
        
        with col_f1:
            selected_category = st.selectbox(
                "Filter by Category",
                ["All"] + sorted(results["categories"].keys())
            )
        
        with col_f2:
            min_size = st.number_input(
                "Minimum Content Size",
                min_value=0,
                value=0,
                step=100
            )
        
        # Filter pages
        filtered_pages = results["pages"]
        if selected_category != "All":
            filtered_pages = [p for p in filtered_pages if p["category"] == selected_category]
        if min_size > 0:
            filtered_pages = [p for p in filtered_pages if p["content_length"] >= min_size]
        
        st.write(f"Showing {len(filtered_pages)} pages")
        
        # Display as table
        pages_df = pd.DataFrame([
            {
                "Title": p["title"][:60],
                "Category": p["category"],
                "Size": f"{p['content_length']:,}",
                "Words": f"{p['word_count']:,}",
                "Routes": p["routes_found"],
                "URL": p["url"]
            }
            for p in filtered_pages
        ])
        
        st.dataframe(pages_df, use_container_width=True, hide_index=True, height=400)
    
    with tab3:
        st.header("API Routes Found")
        
        # Collect all routes
        all_routes = []
        for page in results["pages"]:
            for method, path in page.get("routes", []):
                all_routes.append({
                    "Method": method,
                    "Path": path,
                    "Found On": page["title"][:40],
                    "URL": page["url"]
                })
        
        if all_routes:
            st.write(f"Total API Routes: {len(all_routes)}")
            
            # Method breakdown
            method_counts = defaultdict(int)
            for route in all_routes:
                method_counts[route["Method"]] += 1
            
            col_r1, col_r2 = st.columns([1, 2])
            
            with col_r1:
                st.subheader("By HTTP Method")
                method_df = pd.DataFrame([
                    {"Method": method, "Count": count}
                    for method, count in method_counts.items()
                ])
                st.dataframe(method_df, hide_index=True)
            
            with col_r2:
                fig = px.pie(
                    method_df,
                    values="Count",
                    names="Method",
                    title="Routes by HTTP Method"
                )
                st.plotly_chart(fig, use_container_width=True)
            
            # Filter routes
            st.subheader("All Routes")
            selected_method = st.selectbox(
                "Filter by Method",
                ["All"] + sorted(method_counts.keys())
            )
            
            filtered_routes = all_routes
            if selected_method != "All":
                filtered_routes = [r for r in all_routes if r["Method"] == selected_method]
            
            routes_df = pd.DataFrame(filtered_routes)
            st.dataframe(routes_df, use_container_width=True, hide_index=True, height=400)
        else:
            st.warning("No API routes found")
    
    with tab4:
        st.header("Issues & Skipped URLs")
        
        col_i1, col_i2 = st.columns(2)
        
        with col_i1:
            st.subheader(f"Failed URLs ({results['failed_count']})")
            if results["failed_urls"]:
                failed_df = pd.DataFrame([
                    {
                        "URL": f["url"],
                        "Status/Error": f.get("status") or f.get("error", "Unknown"),
                        "Parent": f.get("parent", "N/A")[:50]
                    }
                    for f in results["failed_urls"]
                ])
                st.dataframe(failed_df, use_container_width=True, hide_index=True)
            else:
                st.success("No failed URLs!")
        
        with col_i2:
            st.subheader(f"Skipped URLs ({results['skipped_count']})")
            if results["skipped_urls"]:
                skipped_df = pd.DataFrame([
                    {
                        "URL": s["url"][:60],
                        "Reason": s["reason"]
                    }
                    for s in results["skipped_urls"]
                ])
                st.dataframe(skipped_df, use_container_width=True, hide_index=True)
            else:
                st.info("No skipped URLs")
    
    with tab5:
        st.header("Export Results")
        
        st.write("Save audit results to files for comparison with your main app.")
        
        if st.button("💾 Save All Reports", type="primary"):
            with st.spinner("Saving reports..."):
                saved_files = save_results(results)
                
                st.success("✅ Reports saved!")
                
                for name, filepath in saved_files.items():
                    st.write(f"**{name.title()}:** `{filepath}`")
                
                # Offer downloads
                st.divider()
                st.subheader("Download Reports")
                
                # Summary JSON
                summary_json = json.dumps({
                    "total_pages": results["total_pages"],
                    "total_routes": results["total_routes"],
                    "categories": results["categories"],
                    "elapsed_time": results["elapsed_time"]
                }, indent=2)
                
                st.download_button(
                    "📄 Download Summary (JSON)",
                    data=summary_json,
                    file_name=f"scrape_summary_{datetime.now().strftime('%Y%m%d_%H%M%S')}.json",
                    mime="application/json"
                )
                
                # Full pages CSV
                pages_csv = pd.DataFrame([
                    {
                        "URL": p["url"],
                        "Title": p["title"],
                        "Category": p["category"],
                        "Content Length": p["content_length"],
                        "Routes Found": p["routes_found"]
                    }
                    for p in results["pages"]
                ]).to_csv(index=False)
                
                st.download_button(
                    "📊 Download Pages (CSV)",
                    data=pages_csv,
                    file_name=f"scrape_pages_{datetime.now().strftime('%Y%m%d_%H%M%S')}.csv",
                    mime="text/csv"
                )


def compare_with_app():
    """Compare audit results with app's scrape"""
    st.header("🔄 Compare with Main App")
    
    # Load expected coverage
    expected_file = OUTPUT_DIR / "expected_coverage.json"
    
    if not expected_file.exists():
        st.warning("⚠️ No audit results found. Run an audit first.")
        return
    
    expected = json.loads(expected_file.read_text())
    
    # Load app metadata
    app_metadata_path = st.text_input(
        "Path to app's scrape_metadata.json",
        value="scrape_metadata.json"
    )
    
    app_file = Path(app_metadata_path)
    
    if st.button("🔍 Compare", type="primary"):
        if not app_file.exists():
            st.error(f"❌ File not found: {app_metadata_path}")
            return
        
        app_data = json.loads(app_file.read_text())
        
        st.success("✅ Comparison Complete")
        
        # Metrics
        col1, col2, col3 = st.columns(3)
        
        expected_pages = expected["expected_minimum_pages"]
        actual_pages = app_data.get("pages_count", 0)
        coverage_pct = (actual_pages / expected_pages * 100) if expected_pages > 0 else 0
        
        col1.metric(
            "Page Coverage",
            f"{coverage_pct:.1f}%",
            delta=f"{actual_pages - expected_pages} pages",
            delta_color="normal" if coverage_pct >= 95 else "inverse"
        )
        
        col2.metric(
            "Expected Pages",
            expected_pages
        )
        
        col3.metric(
            "Actual Pages",
            actual_pages
        )
        
        # Status
        if coverage_pct >= 95:
            st.success("✅ **EXCELLENT** - Your app is scraping 95%+ of the site!")
        elif coverage_pct >= 80:
            st.warning("⚠️ **GOOD** - Your app is scraping 80%+ of the site, but could be better.")
        else:
            st.error("❌ **INCOMPLETE** - Your app is missing significant content.")
        
        # Detailed comparison
        st.divider()
        st.subheader("Detailed Comparison")
        
        comparison_df = pd.DataFrame([
            {
                "Metric": "Pages",
                "Expected (Audit)": expected_pages,
                "Actual (App)": actual_pages,
                "Difference": actual_pages - expected_pages
            },
            {
                "Metric": "Routes (estimated)",
                "Expected (Audit)": expected["expected_minimum_routes"],
                "Actual (App)": "N/A",
                "Difference": "N/A"
            },
            {
                "Metric": "Chunks Created",
                "Expected (Audit)": "N/A",
                "Actual (App)": app_data.get("chunks_count", 0),
                "Difference": "N/A"
            },
            {
                "Metric": "Vectors Stored",
                "Expected (Audit)": "N/A",
                "Actual (App)": app_data.get("vectors_count", 0),
                "Difference": "N/A"
            }
        ])
        
        st.dataframe(comparison_df, use_container_width=True, hide_index=True)
        
        # Recommendations
        if coverage_pct < 95:
            st.divider()
            st.subheader("💡 Recommendations")
            
            if coverage_pct < 80:
                st.markdown("""
                - **Check MAX_PAGES setting** in your main app
                - **Review failed URLs** in the audit results
                - **Verify crawl delay** isn't causing timeouts
                - **Check if some categories are being skipped**
                """)
            else:
                st.markdown("""
                - **Nearly complete!** Just a few pages missing
                - Check the "Issues" tab in the audit for failed URLs
                - Verify your app's URL validation logic matches the auditor
                """)


# Main UI
tab_main, tab_compare = st.tabs(["🚀 Run Audit", "🔄 Compare with App"])

with tab_main:
    st.header("Run Site Audit")
    
    st.markdown(f"""
    This will crawl **{BASE_URL}** and generate a complete report.
    
    **Settings:**
    - Crawl Delay: {crawl_delay}s
    - Max Pages: {'Unlimited' if max_pages is None else max_pages}
    """)
    
    col_btn1, col_btn2 = st.columns(2)
    
    with col_btn1:
        if st.button("🚀 Start Full Audit", type="primary", disabled=st.session_state.crawl_running):
            st.session_state.crawl_running = True
            
            # Progress containers
            progress_bar = st.progress(0)
            status_text = st.empty()
            log_container = st.expander("📋 Crawl Log", expanded=True)
            log_placeholder = log_container.empty()
            logs = []
            
            def update_progress(current, total, url):
                progress = min(current / total, 1.0)
                progress_bar.progress(progress)
                status_text.text(f"Progress: {current}/{total if total else '?'} pages")
            
            def log_status(message):
                logs.append(f"[{datetime.now().strftime('%H:%M:%S')}] {message}")
                log_placeholder.text("\n".join(logs[-20:]))  # Show last 20 logs
            
            # Run audit
            auditor = SiteAuditor(progress_callback=update_progress, status_callback=log_status)
            
            try:
                results = auditor.crawl_all(max_pages=max_pages, crawl_delay=crawl_delay)
                st.session_state.audit_results = results
                
                progress_bar.progress(1.0)
                status_text.success("✅ Audit Complete!")
                
                # Display results
                display_results(results)
                
            except Exception as e:
                st.error(f"❌ Error during audit: {e}")
            
            finally:
                auditor.close()
                st.session_state.crawl_running = False
    
    with col_btn2:
        if st.button("⚡ Quick Test (50 pages)", disabled=st.session_state.crawl_running):
            st.session_state.crawl_running = True
            
            progress_bar = st.progress(0)
            status_text = st.empty()
            
            def update_progress(current, total, url):
                progress = min(current / total, 1.0)
                progress_bar.progress(progress)
                status_text.text(f"Testing: {current}/50 pages")
            
            auditor = SiteAuditor(progress_callback=update_progress)
            
            try:
                results = auditor.crawl_all(max_pages=50, crawl_delay=crawl_delay)
                st.session_state.audit_results = results
                
                progress_bar.progress(1.0)
                status_text.success("✅ Test Complete!")
                
                display_results(results)
                
            except Exception as e:
                st.error(f"❌ Error: {e}")
            
            finally:
                auditor.close()
                st.session_state.crawl_running = False
    
    # Show previous results if available
    if st.session_state.audit_results and not st.session_state.crawl_running:
        st.divider()
        st.subheader("📊 Previous Audit Results")
        display_results(st.session_state.audit_results)

with tab_compare:
    compare_with_app()

# Footer
st.divider()
st.caption("D2L Documentation Scraper Validator v1.0 | Built with Streamlit")
