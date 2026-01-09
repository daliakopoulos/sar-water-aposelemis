# Aposelemis news miner — LLM‑only v2 (Google CSE + GPT, incremental CSV)
# -----------------------------------------------------------------
# What this does
# 1) Uses Google Custom Search JSON API to find pages about Αποσελέμη(ς)
#    that ALSO mention storage-related words (κυβικά, m3, ...).
# 2) Downloads each NEW page (skips anything already present in a CSV log).
# 3) Sends the full article text to the OpenAI Responses API with a strict
#    JSON schema to extract: as_of_date and storage (in hm^3), plus optional %.
# 4) Appends/updates a CSV with columns: Date, Storage, Source.
# Notes
# - This version is *LLM-only*: no regex fallbacks, no Greek date heuristics,
#   no send_to_llm toggle. Cleaner and simpler.
# -----------------------------------------------------------------
# REQUIREMENTS & ENV VARS
# - Needs GOOGLE_API_KEY and GOOGLE_CSE_ID (Google Custom Search JSON API)
# - Needs OPENAI_API_KEY (OpenAI Responses API)
# - Optional: OPENAI_MODEL (defaults to gpt-4o-mini-2024-07-18)
#
# QUICK START
# Sys.setenv(GOOGLE_API_KEY = "...", GOOGLE_CSE_ID = "...")
# Sys.setenv(OPENAI_API_KEY = "...")
# res <- aposelemis_update_csv(csv_path = "aposelemis_storage.csv", pages = 8, num = 10, max_articles = 60)
# View(res)
#
# NOTES
# - Domain filtering also blocks subdomains (e.g., news.example.com) when example.com is listed.
# - Fetching is resilient: retries transient HTTP errors and falls back to AMP if main page is thin.
# - LLM extraction uses a strict JSON Schema via the Responses API (text.format).
# -----------------------------------------------------------------

suppressPackageStartupMessages({
  library(httr2)
  library(jsonlite)
  library(rvest)
  library(xml2)
  library(stringr)
  library(dplyr)
  library(purrr)
  library(tibble)
  library(lubridate)
  library(readr)
})

# ----------------------------
# Config
# ----------------------------
# .USER_AGENT   : Primary UA for polite crawling (custom identifier)
# .BROWSER_UA   : Fallback UA that mimics a real browser (used if sites block the custom UA)
# .SYS_TZ       : Time zone for parsing/normalizing article dates
# .DEFAULT_MODEL: OpenAI model for the Responses API
# .GR_NAME / .GR_STORAGE_TERMS / .DEFAULT_QUERY: Search query parts for Google CSE

.USER_AGENT   <- "aposelemis-news-miner/2.0 (+contact: user)"
.BROWSER_UA   <- "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/125 Safari/537.36"
.SYS_TZ       <- "Europe/Athens"
.DEFAULT_MODEL <- Sys.getenv("OPENAI_MODEL", unset = "gpt-4o-mini")

# Base query components (tweak as desired)
.GR_NAME <- '("Αποσελέμης" OR Αποσελέμη)'
.GR_STORAGE_TERMS <- '(κυβικά OR "κ.μ.")'
.DEFAULT_QUERY <- paste(.GR_NAME, .GR_STORAGE_TERMS)

# ----------------------------
# Helpers
# ----------------------------

# --- helpers to resolve a real page date via HTTP/HTML ---
.http_parse_date <- function(x) {
  if (is.null(x) || is.na(x) || x == "") return(NA)
  x <- trimws(as.character(x))
  # try several formats + simple extraction
  fmts <- c("%Y-%m-%d", "%Y/%m/%d", "%Y-%m-%dT%H:%M:%S", "%Y-%m-%dT%H:%M:%S%z",
            "%a, %d %b %Y %H:%M:%S %Z", "%d %b %Y %H:%M:%S %Z")
  for (f in fmts) {
    dt <- suppressWarnings(as.POSIXct(x, tz = "UTC", format = f))
    if (!is.na(dt)) return(as.Date(dt))
  }
  # extract first YYYY-MM-DD or YYYYMMDD
  m <- regexpr("(\\d{4}-\\d{2}-\\d{2})|(\\d{8})", x)
  if (m > 0) {
    s <- regmatches(x, m)
    if (grepl("-", s)) as.Date(s) else as.Date(s, "%Y%m%d")
  } else NA
}

resolve_page_date_http <- function(url, timeout = http_timeout, ua = .USER_AGENT) {
  # 1) HEAD: Last-Modified
  d_head <- NA
  resp_h <- try(
    httr2::request(url) |>
      httr2::req_user_agent(ua) |>
      httr2::req_timeout(timeout) |>
      httr2::req_method("HEAD") |>
      httr2::req_error(is_error = function(r) FALSE) |>
      httr2::req_perform(),
    silent = TRUE
  )
  if (!inherits(resp_h, "try-error") && !is.null(resp_h)) {
    d_head <- .http_parse_date(httr2::resp_header(resp_h, "last-modified"))
  }
  
  # 2) GET first ~64KB (only proceed if it's HTML)
  resp_g <- try(
    httr2::request(url) |>
      httr2::req_user_agent(ua) |>
      httr2::req_timeout(timeout) |>
      httr2::req_headers("Accept-Language" = "el, en;q=0.8",
                         "Range" = "bytes=0-65535") |>
      httr2::req_error(is_error = function(r) FALSE) |>
      httr2::req_perform(),
    silent = TRUE
  )
  if (inherits(resp_g, "try-error") || is.null(resp_g)) return(d_head)
  
  ct <- httr2::resp_header(resp_g, "content-type")
  if (is.null(ct)) ct <- ""
  ct <- tolower(ct)
  if (!grepl("text/html|application/xhtml\\+xml", ct)) return(d_head)
  
  body <- try(httr2::resp_body_string(resp_g), silent = TRUE)
  if (inherits(body, "try-error") || is.null(body) || identical(body, "")) return(d_head)
  
  doc <- tryCatch(xml2::read_html(body), error = function(e) NULL)
  if (is.null(doc) || !(inherits(doc, "xml_document") || inherits(doc, "xml_node"))) return(d_head)
  
  # meta tags (guarded)
  meta_nodes <- tryCatch(rvest::html_nodes(doc, "meta"), error = function(e) NULL)
  pick_meta <- function(names_or_props) {
    if (is.null(meta_nodes)) return(character(0))
    vals <- character(0)
    nm_name <- tolower(rvest::html_attr(meta_nodes, "name"))
    nm_prop <- tolower(rvest::html_attr(meta_nodes, "property"))
    for (nm in names_or_props) {
      vals <- c(
        vals,
        rvest::html_attr(meta_nodes[nm_name == tolower(nm)], "content"),
        rvest::html_attr(meta_nodes[nm_prop == tolower(nm)], "content")
      )
    }
    unique(na.omit(vals))
  }
  
  candidates <- pick_meta(c(
    "article:published_time","article:modified_time",
    "og:published_time","og:updated_time",
    "date","dc.date","dc.date.issued","dc.date.created",
    "publish-date","pubdate","parsely-pub-date"
  ))
  
  # <time datetime=...>
  time_nodes <- tryCatch(rvest::html_nodes(doc, "time"), error = function(e) NULL)
  if (!is.null(time_nodes)) {
    candidates <- c(candidates, rvest::html_attr(time_nodes, "datetime"))
  }
  
  # JSON-LD (guarded)
  script_nodes <- tryCatch(rvest::html_nodes(doc, 'script[type="application/ld+json"]'), error = function(e) NULL)
  if (!is.null(script_nodes)) {
    for (node in script_nodes) {
      txt <- rvest::html_text(node)
      j <- try(jsonlite::fromJSON(txt, simplifyVector = TRUE), silent = TRUE)
      if (!inherits(j, "try-error") && length(j)) {
        if (is.list(j)) {
          for (f in c("datePublished","dateModified","uploadDate")) {
            v <- try(j[[f]], silent = TRUE)
            if (!inherits(v, "try-error") && !is.null(v)) candidates <- c(candidates, as.character(v))
          }
        }
      }
    }
  }
  
  # First good publish date; else fallback to Last-Modified
  for (c in unique(na.omit(candidates))) {
    d <- .http_parse_date(c)
    if (!is.na(d)) return(d)
  }
  d_head
}



`%||%` <- function(x, y) if (is.null(x) || length(x) == 0) y else x
with_gr_tz <- function(x) with_tz(x, tzone = .SYS_TZ)

sanitize_url <- function(u) {
  u <- as.character(u)
  u[is.na(u) | !nzchar(u)] <- NA_character_
  idx <- !is.na(u)
  u[idx] <- sub("#.+$", "", u[idx])
  u[idx] <- gsub("[?&](utm_[^=&]+=[^&]+)", "", u[idx])
  u[idx] <- sub("[?&]$", "", u[idx])
  u
}

host_of_one <- function(u) {
  u <- as.character(u)
  if (is.na(u) || !nzchar(u)) return(NA_character_)
  out <- tryCatch(httr2::url_parse(u)$hostname, error = function(e) NA_character_)
  if (length(out) == 0 || is.null(out) || !nzchar(out)) NA_character_ else tolower(out)
}
host_of <- function(u) vapply(u, host_of_one, character(1))

# Return TRUE for each host that matches any domain in `domains`,
# including subdomains (e.g., blocking "example.com" also blocks
# "news.example.com").
is_blocked_host <- function(host, domains) {
  host <- tolower(host); domains <- tolower(domains)
  vapply(host, function(h) {
    if (is.na(h) || !nzchar(h)) return(FALSE)
    any(h %in% domains | vapply(domains, function(d) endsWith(h, paste0(".", d)), logical(1)))
  }, logical(1))
}

# Extract main text (DOM + JSON-LD articleBody/description)
# Extract a generous amount of readable text from a news page.
# Strategy:
# 1) Pull visible DOM text from common content containers.
# 2) Also parse JSON-LD blocks and capture `articleBody`/`description`
#    when present (many Greek news sites expose the full body here).
# 3) Normalize whitespace so the LLM sees a clean, compact string.
extract_text <- function(html_doc) {
  nodes <- rvest::html_elements(html_doc, "article, main, section, div, p, li, h1, h2, h3, span, time")
  txt <- paste(rvest::html_text2(nodes), collapse = "\n")
  
  bodies <- character(0)
  scripts <- rvest::html_elements(html_doc, "script[type='application/ld+json']")
  if (length(scripts) > 0) {
    for (sc in scripts) {
      js <- tryCatch(jsonlite::fromJSON(rvest::html_text(sc), simplifyVector = FALSE), error = function(e) NULL)
      if (is.null(js)) next
      objs <- list(js)
      if (is.list(js) && !is.null(js[["@graph"]])) objs <- c(objs, js[["@graph"]])
      for (o in objs) {
        if (is.list(o) && (!is.null(o$articleBody) || !is.null(o$description))) {
          bodies <- c(bodies, as.character(o$articleBody %||% o$description))
        }
      }
    }
  }
  if (length(bodies)) txt <- paste(txt, paste(bodies, collapse = "\n"), sep = "\n")
  
  # whitespace normalize
  txt <- gsub("\u00A0", " ", txt)          # nbsp
  txt <- gsub("[ \t]+", " ", txt)
  txt <- gsub("\n{2,}", "\n", txt)
  trimws(txt)
}

# Robust published datetime from meta / JSON-LD
# Attempt to infer the article's published (or modified) datetime.
# Strategy:
# 1) Check common HTML meta/time tags.
# 2) Scan JSON-LD. We accept any object that exposes date fields
#    (datePublished / uploadDate / dateCreated / dateModified),
#    and we also peek into mainEntityOfPage when types suggest content.
# Edge cases are handled defensively; the function never throws.
extract_meta_datetime <- function(doc) {
  # 1) meta tags
  sels <- c(
    "meta[property='article:published_time']",
    "meta[name='article:published_time']",
    "meta[property='article:modified_time']",
    "meta[property='og:article:published_time']",
    "meta[property='og:updated_time']",
    "meta[itemprop='datePublished']",
    "meta[itemprop='dateModified']",
    "meta[name='pubdate']",
    "meta[name='date']",
    "time[datetime]"
  )
  for (s in sels) {
    n <- rvest::html_element(doc, s)
    if (length(n) > 0) {
      val <- rvest::html_attr(n, "content")
      if (length(val) == 0 || is.na(val) || !nzchar(val)) val <- rvest::html_attr(n, "datetime")
      if (length(val) > 0 && !is.na(val) && nzchar(val)) {
        dt <- suppressWarnings(lubridate::parse_date_time(
          val,
          orders = c("Ymd HMS","Y-m-d H:M:S","Ymd HM","Ymd","dmy HMS","dmy HM","dmy","ymd"),
          tz = .SYS_TZ
        ))
        if (!is.na(dt)) return(with_gr_tz(dt))
      }
    }
  }
  # 2) JSON-LD (accept any object with usable date fields)
  scripts <- rvest::html_elements(doc, "script[type='application/ld+json']")
  if (length(scripts) > 0) {
    for (sc in scripts) {
      txt <- rvest::html_text(sc)
      js  <- tryCatch(jsonlite::fromJSON(txt, simplifyVector = FALSE), error = function(e) NULL)
      if (is.null(js)) next
      objs <- list(js)
      if (is.list(js) && !is.null(js[["@graph"]])) objs <- c(objs, js[["@graph"]])
      for (o in objs) {
        if (!is.list(o)) next
        types   <- tryCatch(as.character(unlist(o[["@type"]])), error = function(e) character(0))
        ok_type <- isTRUE(any(na.omit(types) %in% c("NewsArticle","Article","BlogPosting","WebPage","CreativeWork")))
        dt_raw  <- o$datePublished %||% o$uploadDate %||% o$dateCreated %||% o$dateModified
        if (is.null(dt_raw) && ok_type) {
          dt_raw <- tryCatch(o$mainEntityOfPage$datePublished %||% o$mainEntityOfPage$dateModified, error = function(e) NULL)
        }
        if (!is.null(dt_raw)) {
          dtp <- suppressWarnings(lubridate::parse_date_time(
            as.character(dt_raw),
            orders = c("Ymd HMS","Y-m-d H:M:S","Ymd HM","Ymd","dmy HMS","dmy HM","dmy","ymd"),
            tz = .SYS_TZ
          ))
          if (!is.na(dtp)) return(with_gr_tz(dtp))
        }
      }
    }
  }
  NA
}

# If the page links an AMP version, return its URL (often cleaner text).
get_amp_url <- function(doc) {
  amp <- rvest::html_attr(rvest::html_element(doc, "link[rel='amphtml']"), "href")
  if (!is.na(amp) && nzchar(amp)) sanitize_url(amp) else NA_character_
}

# Download and parse a single article URL while being resilient to
# common news-site behaviors (soft blocks, heavy JS, thin main HTML).
# Steps:
# - Try with custom UA, then fall back to a browser UA.
# - Retry transient HTTP errors (408/429/5xx) with exponential backoff.
# - Parse HTML; if the text looks thin, try the AMP version.
# - Always return a tibble row (unless HTTP/parse totally fails), so
#   downstream LLM extraction has something to work with.
fetch_article <- function(url) {
  u <- sanitize_url(url)
  
  do_fetch <- function(u, ua) {
    httr2::request(u) |>
      httr2::req_user_agent(ua) |>
      httr2::req_headers(
        `Accept-Language` = "el, en;q=0.8",
        `Accept` = "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8"
      ) |>
      httr2::req_timeout(20) |>
      httr2::req_retry(
        max_tries = 3,
        backoff = ~ runif(1, 0.5, 1.2) * (2^(..try - 1)),
        is_transient = function(r) httr2::resp_status(r) %in% c(408,429,500,502,503,504)
      ) |>
      httr2::req_error(is_error = function(r) FALSE) |>
      httr2::req_perform()
  }
  
  # Try with your UA; fall back to a browser UA
  resp <- tryCatch(do_fetch(u, .USER_AGENT), error = function(e) NULL)
  if (is.null(resp) || httr2::resp_status(resp) >= 400) {
    resp <- tryCatch(do_fetch(u, .BROWSER_UA), error = function(e) NULL)
  }
  if (is.null(resp) || httr2::resp_status(resp) >= 400) return(NULL)
  
  body <- httr2::resp_body_string(resp)
  doc  <- tryCatch(xml2::read_html(body), error = function(e) NULL)
  if (is.null(doc)) return(NULL)
  
  title <- tryCatch(rvest::html_text2(rvest::html_element(doc, "title")), error=function(e) NA_character_)
  pub   <- tryCatch(extract_meta_datetime(doc), error=function(e) NA)
  text  <- tryCatch(extract_text(doc), error=function(e) NA_character_)
  
  # If text thin, try AMP
  if (length(text) == 0 || is.na(text) || nchar(text) < 800) {
    amp <- get_amp_url(doc)
    if (!is.na(amp)) {
      resp2 <- tryCatch(do_fetch(amp, .BROWSER_UA), error = function(e) NULL)
      if (!is.null(resp2) && httr2::resp_status(resp2) < 400) {
        doc2 <- tryCatch(xml2::read_html(httr2::resp_body_string(resp2)), error = function(e) NULL)
        if (!is.null(doc2)) {
          pub  <- tryCatch(extract_meta_datetime(doc2), error=function(e) pub)
          text <- tryCatch(extract_text(doc2),        error=function(e) text)
        }
      }
    }
  }
  
  # Last resort: keep some non-empty text so the row survives
  if (is.na(text) || !nzchar(text)) text <- substr(gsub("\\s+", " ", body), 1, 20000)
  
  tibble::tibble(
    url   = u,
    host  = host_of_one(u),
    title = title,
    published_at_guess = pub,
    text  = text
  )
}

# ----------------------------
# Google Custom Search (JSON API)
# ----------------------------

# Normalize Google CSE `items` payload into a tidy tibble regardless
# of how the JSON was flattened by httr2/jsonlite.
items_to_df <- function(items) {
  if (is.null(items)) return(tibble::tibble())
  if (is.data.frame(items)) {
    rows <- split(items, seq_len(nrow(items)))
  } else if (is.list(items)) {
    if (length(items) && is.list(items[[1]])) {
      rows <- items
    } else {
      ti <- items[["title"]]; ln <- items[["link"]]; sn <- items[["snippet"]]
      if (!is.null(ti) || !is.null(ln) || !is.null(sn)) {
        n <- max(length(ti %||% character()), length(ln %||% character()), length(sn %||% character()))
        return(tibble::tibble(
          title    = as.character((ti %||% rep(NA_character_, n))[seq_len(n)]),
          url      = as.character((ln %||% rep(NA_character_, n))[seq_len(n)]),
          snippet  = as.character((sn %||% rep(NA_character_, n))[seq_len(n)]),
          provider = "google_cse"
        ))
      }
      rows <- list()
    }
  } else {
    rows <- list()
  }
  if (!length(rows)) return(tibble::tibble())
  
  tibble::tibble(
    title    = vapply(rows, function(r) as.character((r[["title"]]       %||% r[["htmlTitle"]]    %||% NA_character_)[1]), character(1)),
    url      = vapply(rows, function(r) as.character((r[["link"]]        %||% r[["formattedUrl"]] %||% NA_character_)[1]), character(1)),
    snippet  = vapply(rows, function(r) as.character((r[["snippet"]]     %||% r[["htmlSnippet"]]  %||% NA_character_)[1]), character(1)),
    provider = "google_cse"
  )
}

# Query Google Custom Search API for result pages.
# - Paginates through `pages` * `num` results (CSE caps usable results).
# - Normalizes URLs, extracts host, and filters out blocked domains
#   (including subdomains via `is_blocked_host`).
# - Returns unique rows by URL.
search_google_cse <- function(
    query = .DEFAULT_QUERY,
    pages = 5,                  # max pages to try (10 results per page)
    num = 10,                   # keep at 10; JSON API max
    lr = "lang_el",
    year = NULL,                # e.g. 2024 (overrides date_from/to)
    date_from = NULL,           # "YYYY-MM-DD"
    date_to   = NULL,           # "YYYY-MM-DD"
    keep_undated = FALSE,       # keep items without a reliable date
    min_inrange = 25,           # stop early once we collect this many in-range items
    max_empty_pages = 2,        # stop if this many pages add 0 in-range items
    resolve_http_dates = FALSE, # fetch page to resolve real publish date (slower)
    http_timeout = 6,           # seconds per URL when resolving HTTP dates
    verbose = TRUE,
    exclude_domains = c(
      "youtube.com","youtu.be","facebook.com","instagram.com",
      "x.com","twitter.com","oakae.gr","hellenicparliament.gr","el.wikipedia.org"
    )
) {
  key <- Sys.getenv("GOOGLE_API_KEY"); cx <- Sys.getenv("GOOGLE_CSE_ID")
  if (key == "" || cx == "") stop("Set GOOGLE_API_KEY and GOOGLE_CSE_ID env vars.")
  
  # ---------- local helpers ----------
  ymd_nodash <- function(x) gsub("-", "", x)
  
  # Parse best-available date from a single 'pagemap' object (JSON API field)
  parse_item_date <- function(pm) {
    if (is.null(pm)) return(NA_real_)
    grab <- function(lst, keys) {
      for (k in keys) {
        v <- lst[[k]]
        if (!is.null(v) && length(v) >= 1) return(as.character(v[[1]]))
      }
      NULL
    }
    candidates <- character(0)
    for (node in c("newsarticle","blogposting","scholarlyarticle","article","webpage")) {
      if (!is.null(pm[[node]])) {
        for (obj in pm[[node]]) {
          candidates <- c(candidates, grab(obj, c(
            "datePublished","datepublished","dateCreated","datecreated","dateModified","datemodified"
          )))
        }
      }
    }
    if (!is.null(pm$metatags)) {
      for (mt in pm$metatags) {
        candidates <- c(candidates, unlist(mt[c(
          "article:published_time","article:modified_time",
          "og:updated_time","og:published_time",
          "date","dc.date","dc.date.issued","dc.date.created",
          "publish-date","pubdate","parsely-pub-date"
        )], use.names = FALSE))
      }
    }
    candidates <- unique(na.omit(candidates))
    if (!length(candidates)) return(NA_real_)
    
    parse_one <- function(x) {
      x <- trimws(x)
      suppressWarnings({
        dt <- tryCatch({
          if (grepl("^\\d{8}$", x)) return(as.Date(x, "%Y%m%d"))
          if (grepl("^\\d{4}-\\d{2}-\\d{2}$", x)) return(as.Date(x))
          if (grepl("^\\d{4}-\\d{2}-\\d{2}T", x)) return(as.Date(substr(x, 1, 10)))
          if (grepl("^[A-Za-z]{3,} \\d{1,2}, \\d{4}", x))
            return(as.Date(x, tryFormats = c("%B %d, %Y", "%b %d, %Y")))
          m <- regexpr("(\\d{4}-\\d{2}-\\d{2})|(\\d{8})", x)
          if (m > 0) {
            s <- regmatches(x, m)
            if (grepl("-", s)) as.Date(s) else as.Date(s, "%Y%m%d")
          } else NA
        }, error = function(e) NA)
      })
      dt
    }
    for (cand in candidates) {
      d <- parse_one(cand)
      if (!is.na(d)) return(as.numeric(d)) # store numeric for speed
    }
    NA_real_
  }
  
  # --- helpers to resolve a real page date via HTTP/HTML (optional) ---
  .http_parse_date <- function(x) {
    if (is.null(x) || is.na(x) || x == "") return(NA)
    s <- trimws(as.character(x))
    
    # ISO-8601: normalize Z/offsets and strip fractional seconds
    if (grepl("^\\d{4}-\\d{2}-\\d{2}", s)) {
      s2 <- sub("\\.\\d+", "", s)
      s2 <- sub("Z$", "+0000", s2, ignore.case = TRUE)
      s2 <- sub("([+-]\\d{2}):(\\d{2})$", "\\1\\2", s2) # +hh:mm -> +hhmm
      if (grepl("^\\d{4}-\\d{2}-\\d{2}$", s2)) return(as.Date(s2))
      suppressWarnings({
        dt <- try(as.POSIXct(s2, format = "%Y-%m-%dT%H:%M:%S%z", tz = "UTC"), silent = TRUE)
        if (!inherits(dt, "try-error") && !is.na(dt)) return(as.Date(dt))
        dt <- try(as.POSIXct(substr(s2, 1, 19), format = "%Y-%m-%dT%H:%M:%S", tz = "UTC"), silent = TRUE)
        if (!inherits(dt, "try-error") && !is.na(dt)) return(as.Date(dt))
      })
    }
    
    # RFC-1123-like: "Mon, 02 Jan 2006 15:04:05 GMT" (drop tz abbrev, avoid %Z)
    s3 <- gsub("\\s*\\([^)]+\\)$", "", s)     # remove "(EEST)" etc.
    s3 <- sub("\\s+[A-Za-z]{2,5}$", "", s3)   # drop " GMT"
    suppressWarnings({
      dt <- try(as.POSIXct(s3, format = "%a, %d %b %Y %H:%M:%S", tz = "UTC"), silent = TRUE)
      if (!inherits(dt, "try-error") && !is.na(dt)) return(as.Date(dt))
      dt <- try(as.POSIXct(s3, format = "%d %b %Y %H:%M:%S", tz = "UTC"), silent = TRUE)
      if (!inherits(dt, "try-error") && !is.na(dt)) return(as.Date(dt))
    })
    
    # Simple forms
    if (grepl("^\\d{4}/\\d{2}/\\d{2}$", s)) return(as.Date(s, "%Y/%m/%d"))
    if (grepl("^[A-Za-z]{3,} \\d{1,2}, \\d{4}", s)) {
      suppressWarnings({
        dt <- try(as.Date(s, tryFormats = c("%B %d, %Y", "%b %d, %Y")), silent = TRUE)
      })
      if (!inherits(dt, "try-error") && !is.na(dt)) return(dt)
    }
    
    # Fallback: first YYYY-MM-DD or YYYYMMDD inside the string
    m <- regexpr("(\\d{4}-\\d{2}-\\d{2})|(\\d{8})", s)
    if (m > 0) {
      seg <- regmatches(s, m)
      return(if (grepl("-", seg)) as.Date(seg) else as.Date(seg, "%Y%m%d"))
    }
    NA
  }
  
  resolve_page_date_http <- function(url, timeout = http_timeout, ua = .USER_AGENT) {
    # 1) HEAD: Last-Modified
    lm <- try({
      resp_h <- httr2::request(url) |>
        httr2::req_user_agent(ua) |>
        httr2::req_timeout(timeout) |>
        httr2::req_method("HEAD") |>
        httr2::req_error(is_error = function(r) FALSE) |>
        httr2::req_perform()
      httr2::resp_header(resp_h, "last-modified")
    }, silent = TRUE)
    d_head <- .http_parse_date(if (inherits(lm, "try-error")) NA else lm)
    
    # 2) GET first ~64KB and parse HTML metadata
    body <- try({
      resp_g <- httr2::request(url) |>
        httr2::req_user_agent(ua) |>
        httr2::req_timeout(timeout) |>
        httr2::req_headers("Accept-Language" = "el, en;q=0.8", "Range" = "bytes=0-65535") |>
        httr2::req_error(is_error = function(r) FALSE) |>
        httr2::req_perform()
      httr2::resp_body_string(resp_g)
    }, silent = TRUE)
    if (inherits(body, "try-error") || is.null(body)) return(d_head)
    
    doc <- try(xml2::read_html(body), silent = TRUE)
    if (inherits(doc, "try-error")) return(d_head)
    
    meta_nodes <- rvest::html_nodes(doc, "meta")
    pick_meta <- function(names_or_props) {
      vals <- character(0)
      for (nm in names_or_props) {
        nodes1 <- meta_nodes[tolower(rvest::html_attr(meta_nodes, "name")) == tolower(nm)]
        vals   <- c(vals, rvest::html_attr(nodes1, "content"))
        nodes2 <- meta_nodes[tolower(rvest::html_attr(meta_nodes, "property")) == tolower(nm)]
        vals   <- c(vals, rvest::html_attr(nodes2, "content"))
      }
      unique(na.omit(vals))
    }
    
    candidates <- c(
      pick_meta(c(
        "article:published_time","article:modified_time",
        "og:published_time","og:updated_time",
        "date","dc.date","dc.date.issued","dc.date.created",
        "publish-date","pubdate","parsely-pub-date"
      ))
    )
    
    time_nodes <- rvest::html_nodes(doc, "time")
    candidates <- c(candidates, rvest::html_attr(time_nodes, "datetime"))
    
    for (node in rvest::html_nodes(doc, 'script[type="application/ld+json"]')) {
      txt <- rvest::html_text(node)
      j <- try(jsonlite::fromJSON(txt, simplifyVector = TRUE), silent = TRUE)
      if (!inherits(j, "try-error") && length(j)) {
        if (is.list(j)) {
          for (f in c("datePublished","dateModified","uploadDate")) {
            v <- try(j[[f]], silent = TRUE)
            if (!inherits(v, "try-error") && !is.null(v)) candidates <- c(candidates, as.character(v))
          }
        }
      }
    }
    
    for (c in unique(na.omit(candidates))) {
      d <- .http_parse_date(c)
      if (!is.na(d)) return(d)
    }
    d_head
  }
  
  # ---------- build date window & Google-side range ----------
  if (!is.null(year)) {
    if (!is.numeric(year) || nchar(as.character(year)) != 4)
      stop("`year` must be a 4-digit number.")
    date_from <- sprintf("%d-01-01", year)
    date_to   <- sprintf("%d-12-31", year)
  }
  
  sort_param <- NULL
  if (!is.null(date_from) || !is.null(date_to)) {
    from <- if (!is.null(date_from)) ymd_nodash(as.character(date_from)) else ""
    to   <- if (!is.null(date_to))   ymd_nodash(as.character(date_to))   else ""
    sort_param <- sprintf("date:r:%s:%s", from, to)
  }
  
  from_d <- if (!is.null(date_from)) as.Date(date_from) else as.Date("1900-01-01")
  to_d   <- if (!is.null(date_to))   as.Date(date_to)   else as.Date("9999-12-31")
  use_window <- !is.null(date_from) || !is.null(date_to)
  
  # ---------- paging loop with early stop ----------
  out <- list()
  inrange_total <- 0L
  empty_streak <- 0L
  starts <- seq(1, by = num, length.out = pages)  # up to ~100 results total
  
  for (i in seq_along(starts)) {
    s <- starts[i]
    req <- httr2::request("https://www.googleapis.com/customsearch/v1") |>
      httr2::req_user_agent(.USER_AGENT) |>
      httr2::req_url_query(key = key, cx = cx, q = query, num = num, start = s, hl = "el", gl = "gr", lr = lr)
    if (!is.null(sort_param)) req <- req |> httr2::req_url_query(sort = sort_param)
    req <- req |>
      httr2::req_retry(
        max_tries = 3,
        backoff = ~ 0.4 * (2^(..try-1)),
        is_transient = function(r) httr2::resp_status(r) %in% c(408,429,500,502,503,504)
      ) |>
      httr2::req_error(is_error = function(r) FALSE)
    
    resp <- tryCatch(httr2::req_perform(req), error = function(e) NULL)
    if (is.null(resp) || httr2::resp_status(resp) >= 400) next
    
    js <- httr2::resp_body_json(resp, simplifyVector = FALSE)
    items <- js$items
    if (is.null(items)) next
    
    df_items <- items_to_df(items)
    
    urls <- vapply(items, function(it) {
      if (!is.null(it$link)) it$link else if (!is.null(it$url)) it$url else NA_character_
    }, character(1))
    
    dates_num <- vapply(items, function(it) parse_item_date(it$pagemap), numeric(1))
    df_dates <- tibble::tibble(url = urls, page_date = as.Date(dates_num, origin = "1970-01-01"))
    df_page <- dplyr::left_join(df_items, df_dates, by = "url")
    
    # count in-range items on this page (only if a window is set)
    inrange_page <- if (use_window) {
      sum(!is.na(df_page$page_date) & df_page$page_date >= from_d & df_page$page_date <= to_d) +
        if (isTRUE(keep_undated)) sum(is.na(df_page$page_date)) else 0L
    } else 0L
    
    out[[length(out)+1]] <- df_page
    inrange_total <- inrange_total + inrange_page
    empty_streak <- if (inrange_page == 0) empty_streak + 1L else 0L
    
    if (isTRUE(verbose)) message(sprintf("Page %d: %d in-range (cum %d).", i, inrange_page, inrange_total))
    if (use_window && !is.na(min_inrange) && inrange_total >= min_inrange) break
    if (empty_streak >= max_empty_pages) break
  }
  
  if (!length(out)) return(tibble::tibble())
  
  res <- dplyr::bind_rows(out) |>
    dplyr::mutate(url = sanitize_url(url), host = host_of(url)) |>
    dplyr::filter(!is.na(host)) |>
    dplyr::filter(!is_blocked_host(host, exclude_domains))
  
  # ---------- (optional) resolve real publish dates via HTTP/HTML ----------
  if (isTRUE(resolve_http_dates)) {
    target <- if (use_window) {
      is.na(res$page_date) | res$page_date < from_d | res$page_date > to_d
    } else {
      is.na(res$page_date)
    }
    if (any(target)) {
      urls_to_fix <- res$url[target]
      fixed <- vapply(
        urls_to_fix,
        function(u) {
          tryCatch(
            resolve_page_date_http(u, timeout = http_timeout),
            error = function(e) as.Date(NA)
          )
        },
        as.Date(NA)
      )
      res$page_date[target] <- fixed
    }
  }
  
  # ---------- final strict date filter ----------
  if (use_window) {
    res <- if (keep_undated) {
      dplyr::filter(res, is.na(page_date) | (page_date >= from_d & page_date <= to_d))
    } else {
      dplyr::filter(res, !is.na(page_date) & page_date >= from_d & page_date <= to_d)
    }
  }
  
  dplyr::distinct(res, url, .keep_all = TRUE)
}



# ----------------------------
# OpenAI Responses API extraction (strict JSON schema)
# ----------------------------

# Call the OpenAI Responses API with a strict JSON Schema and ask for
# structured extraction from the provided article text.
# Notes:
# - Uses `text.format = json_schema` per the latest Responses API.
# - `required` includes all keys; values may still be `null`.
# - We truncate input text to ~12k chars to keep token use reasonable.
# - Multiple paths are tried to read the model's JSON string robustly.
openai_extract <- function(text, url = NULL, model = .DEFAULT_MODEL) {
  key <- Sys.getenv("OPENAI_API_KEY")
  if (key == "") stop("Set OPENAI_API_KEY env var.")
  
  prompt <- paste0(
    "Extract factual fields from this Greek article about the Αποσελέμης/Αποσελέμη dam.\n",
    "Return STRICT JSON conforming to the schema.\n",
    "Rules: as_of_date MUST be ISO date YYYY-MM-DD.\n",
    "storage_hm3 MUST be numeric in hm^3 (million m^3). Convert units if needed.\n",
    "Prefer in-reservoir storage (όγκος/κυβικά στον ταμιευτήρα) over inflows/outflows.\n",
    if (!is.null(url)) paste0("Source URL: ", url, "\n\n") else "",
    "Text (trimmed):\n",
    substr(text, 1, 12000)
  )
  
  schema_obj <- list(
    type = "object",
    additionalProperties = FALSE,
    properties = list(
      as_of_date     = list(type = c("string","null"), pattern = "^[0-9]{4}-[0-9]{2}-[0-9]{2}$"),
      storage_hm3    = list(type = c("number","null")),
      percent_full   = list(type = c("number","null")),
      unit_mentioned = list(type = c("string","null")),
      raw_quote      = list(type = c("string","null"))
    ),
    required = c("as_of_date","storage_hm3","percent_full","unit_mentioned","raw_quote")
  )
  
  body <- list(
    model = model,
    input = list(list(role = "user", content = prompt)),
    text  = list(
      format = list(
        type   = "json_schema",
        name   = "aposelemis_article",
        schema = schema_obj,
        strict = TRUE
      )
    ),
    temperature = 0,
    top_p = 1,
    max_output_tokens = 400
  )
  
  resp <- httr2::request("https://api.openai.com/v1/responses") |>
    httr2::req_headers(Authorization = paste("Bearer", key)) |>
    httr2::req_user_agent(.USER_AGENT) |>
    httr2::req_body_json(body) |>
    httr2::req_error(is_error = function(r) FALSE) |>
    httr2::req_perform()
  
  if (httr2::resp_status(resp) >= 400) {
    stop(sprintf("OpenAI error %s: %s", httr2::resp_status(resp), httr2::resp_body_string(resp)))
  }
  
  js  <- httr2::resp_body_json(resp, simplifyVector = FALSE)
  txt <- purrr::pluck(js, "output", 1, "content", 1, "text", .default = NULL)
  if (is.null(txt)) txt <- js$output_text %||% purrr::pluck(js, "choices", 1, "message", "content")
  
  out <- tryCatch(jsonlite::fromJSON(txt), error = function(e) NULL)
  if (is.null(out)) out <- list(as_of_date = NA_character_, storage_hm3 = NA_real_, percent_full = NA_real_, unit_mentioned = NA_character_, raw_quote = NA_character_)
  out
}

# ----------------------------
# CSV helpers
# ----------------------------

# Create the output CSV if missing (with fixed column types).
.ensure_csv <- function(csv_path) {
  if (!file.exists(csv_path)) {
    write_csv(tibble(Date = as.Date(character()), Storage = numeric(), Source = character()), csv_path)
  }
}

# Read existing CSV with explicit col types to avoid dplyr type conflicts.
.read_existing <- function(csv_path) {
  suppressWarnings(
    readr::read_csv(
      csv_path,
      show_col_types = FALSE,
      col_types = readr::cols(
        Date    = readr::col_date(),
        Storage = readr::col_double(),
        Source  = readr::col_character()
      )
    )
  )
}

# ----------------------------
# Main: incremental CSV updater (LLM-only)
# ----------------------------

# End-to-end pipeline:
# 1) Read existing CSV (to skip already-processed URLs).
# 2) Search Google CSE (plus optional `extra_urls`).
# 3) Fetch each new URL (tolerant; AMP fallback when thin).
# 4) Send article text to the LLM extractor (strict JSON Schema).
# 5) Choose Date = LLM-as-of-date (preferred) else meta date.
# 6) Append rows and de-duplicate by Source, keeping the best info.
aposelemis_update_csv <- function(csv_path = "aposelemis_storage.csv",
                                  query = .DEFAULT_QUERY,
                                  pages = 5, num = 10,
                                  max_articles = 100,
                                  extra_urls = NULL,
                                  from_date = NULL, to_date = NULL, year = NULL,
                                  exclude_domains = c("youtube.com","youtu.be","facebook.com","instagram.com","x.com","twitter.com","oakae.gr","hellenicparliament.gr","el.wikipedia.org")) {
  .ensure_csv(csv_path)
  existing <- .read_existing(csv_path)
  known_urls <- unique(existing$Source)
  
  # normalize dates to character "YYYY-MM-DD" if user passed Date objects
  if (!is.null(from_date)) from_date <- as.character(as.Date(from_date))
  if (!is.null(to_date))   to_date   <- as.character(as.Date(to_date))
  
  hits <- search_google_cse(
    query = query,
    pages = pages,
    num = num,
    lr = "lang_el",
    year = year,                  # <- NEW: calendar year (overrides dates if given)
    date_from = from_date,        # <- NEW: "YYYY-MM-DD"
    date_to   = to_date,          # <- NEW: "YYYY-MM-DD"
    keep_undated = FALSE,         # drop items we can’t date
    resolve_http_dates = TRUE,    # mimic UI custom range (can set FALSE for speed)
    exclude_domains = exclude_domains
  )
  
 

  
  # Force-inject specific URLs if needed
  if (!is.null(extra_urls) && length(extra_urls)) {
    extra <- tibble(title = NA_character_, url = sanitize_url(extra_urls), snippet = NA_character_, provider = "manual")
    hits <- dplyr::bind_rows(hits, extra) |> dplyr::distinct(url, .keep_all = TRUE)
  }
  
  if (!nrow(hits)) {
    message("No search results.")
    return(existing)
  }
  
  hits <- hits |>
    mutate(host = host_of(url)) |>
    filter(!(url %in% known_urls)) |>
    filter(!is_blocked_host(host, exclude_domains)) |>
    slice_head(n = max_articles)
  
  if (!nrow(hits)) {
    message("No new URLs to process.")
    return(existing)
  }
  
  safe_fetch <- purrr::possibly(fetch_article, otherwise = NULL)
  articles <- purrr::map(hits$url, safe_fetch)
  ok <- !vapply(articles, is.null, logical(1))
  arts <- purrr::list_rbind(articles[ok])
  if (!nrow(arts)) {
    message("Failed to fetch new articles.")
    return(existing)
  }
  
  # LLM extraction per article (LLM-only path)
  llm <- purrr::map(seq_len(nrow(arts)), function(i) {
    tryCatch(
      openai_extract(arts$text[i], arts$url[i]),
      error = function(e) list(as_of_date = NA_character_, storage_hm3 = NA_real_, percent_full = NA_real_, unit_mentioned = NA_character_, raw_quote = NA_character_)
    )
  })
  
  out <- arts |>
    mutate(
      llm_as_of_date  = vapply(llm, function(x) x$as_of_date %||% NA_character_, ""),
      llm_storage_hm3 = vapply(llm, function(x) as.numeric(x$storage_hm3 %||% NA_real_), numeric(1)),
      Date_llm  = suppressWarnings(as.Date(llm_as_of_date)),
      Date_meta = as_date(published_at_guess),
      Date      = dplyr::coalesce(Date_llm, Date_meta),
      Storage   = dplyr::na_if(llm_storage_hm3, 0),  # treat exact 0 as missing
      Source    = url
    )
  
  # Optional publication window
  if (!is.null(from_date)) out <- out |> dplyr::filter(!is.na(Date) & Date >= as.Date(from_date))
  if (!is.null(to_date))   out <- out |> dplyr::filter(!is.na(Date) & Date <= as.Date(to_date))
  
  out <- out |>
    select(Date, Storage, Source) |>
    filter(!is.na(Date) | !is.na(Storage))
  
  if (!nrow(out)) {
    message("LLM returned no usable rows.")
    return(existing)
  }
  
  combined <- bind_rows(existing, out) |>
    group_by(Source) |>
    arrange(desc(!is.na(Storage)), desc(!is.na(Date))) |>
    slice(1) |>
    ungroup() |>
    arrange(Date, Source) |>
    select(Date, Storage, Source)
  
  write_csv(combined, csv_path)
  combined
}

# ----------------------------
# Example run
# ----------------------------
# Sys.setenv(GOOGLE_API_KEY = "...", GOOGLE_CSE_ID = "...")
# Sys.setenv(OPENAI_API_KEY = "...")
# res <- aposelemis_update_csv(
#   csv_path = "aposelemis_storage.csv",
#   pages = 8, num = 10, max_articles = 60
# )
# View(res)


  
# ----------------------------
# The run
# ----------------------------

source("Aposelemis_dam_data_mining_api_keys.R")
#Sys.setenv(GOOGLE_API_KEY = "xxx", GOOGLE_CSE_ID = "yyy")
#Sys.setenv(OPENAI_API_KEY = "zzz")


years <- 2015:2025
res_list <- vector("list", length(years)); names(res_list) <- years

for (i in seq_along(years)) {
  y <- years[i]
  message(sprintf("=== Updating %d ===", y))
  res_list[[i]] <- try(
    aposelemis_update_csv(
      csv_path = "aposelemis_mined_storage.csv",
      pages = 8, num = 10, max_articles = 100,
      year = y
    ),
    silent = TRUE
  )
  if (inherits(res_list[[i]], "try-error")) {
    warning(sprintf("Year %d failed: %s", y, as.character(res_list[[i]])))
  } else {
    message(sprintf("Year %d: %s rows returned.", y, nrow(res_list[[i]])))
  }
  Sys.sleep(1)  # play nice with quotas/rate limits
}

# Optional: combine the results you got back from each year (if the function returns a data frame)
all_new <- do.call(dplyr::bind_rows, Filter(function(x) !inherits(x, "try-error"), res_list))


View(res)
  
 

  