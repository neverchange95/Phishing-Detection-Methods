import argparse
import ipaddress
import re
import urllib

import tldextract
from pathlib import Path
from typing import List
from urllib.parse import urlparse

import pandas as pd

CREDENTIAL_KEYWORDS: list[str] = [
    "user", "username", "uid",
    "password", "pwd", "secret",
    "key", "token", "cred"
]

SUSPICIOUS_KEYWORDS: list[str] = [
    "login", "signin", "log-in",
    "sign-in", "account", "verify",
    "secure", "update", "confirm",
    "webscr", "safe", "secure",
    "ssl", "security", "admin",
    "protection", "bank", "credit",
    "payment", "invoice", "bill",
    "checkout", "transfer", "support"
]

KNOWN_SHORTENING_SERVICES: list[str] = [
    "bit.ly", "tinyurl.com", "goo.gl",
    "t.co", "t.ly", "ow.ly",
    "cutt.ly", "is.gd", "v.gd",
    "buff.ly", "rebrand.ly", "shorturl.at",
    "shrtco.de", "rb.g", "tiny.cc",
    "youtu.be", "lnkd.in", "wp.me",
    "amzn.to", "g.co"
]


def read_urls_from_csv(csv_path: Path) -> List[str]:
    """
    reads urls from a csv file, and returns list of that urls.
    """
    if not csv_path.is_file():
        raise FileNotFoundError(f"File {csv_path} does not exist")

    # read csv
    df = pd.read_csv(csv_path, dtype=str)

    # find url column
    candidates = [c for c in df.columns if c.lower() == "url"]
    if not candidates:
        raise KeyError(f"No urls found in {csv_path}")

    # return list of urls
    url_col = candidates[0]
    series = df[url_col].dropna().astype(str).map(str.strip)
    return [u for u in series if u != ""]


# Feature 1
def url_char_count(url: str) -> int:
    """
    returns number of characters in an url
    """
    return len(url)


# Feature 2
def url_slash_count(url: str) -> int:
    """
    returns number of slashes in an url
    """
    return url.count("/")


# Feature 3
def check_https(url: str) -> int:
    """
    checks if an url use https
    """
    scheme = urlparse(url).scheme.lower()
    if scheme == "https":
        return 1
    else:
        return 0


# Feature 4
def http_occurrence_count(url: str) -> int:
    """
    returns number of 'http://' occurrences in an url
    """
    haystack = url.lower()
    needle = "http://"
    return haystack.count(needle)


# Feature 5
def https_occurrence_count(url: str) -> int:
    """
    returns number of 'https://' occurrences in an url
    """
    haystack = url.lower()
    needle = "https://"
    return haystack.count(needle)


# Feature 6
def url_dot_count(url: str) -> int:
    """
    returns number of '.' occurrences in an url
    """
    return url.count(".")


# Feature 7
def check_ip(url: str) -> int:
    """
    checks if url refers to an ip address rather than a domain name
    """
    # extract domain from url
    parts = urlparse(url)
    domain = parts.netloc

    if parts.scheme is None:
        # no scheme available, try again with default http scheme.
        parts = urlparse("http://" + url)
        domain = parts.netloc

    # check if domain is an ip
    try:
        ipaddress.ip_address(domain)
        return 1
    except ValueError:
        return 0


# Feature 8
def url_digit_count(url: str) -> int:
    """
    returns number of digits in an url
    """
    return len(re.findall(r"[0-9]", url))


# Feature 9
def url_dash_count(url: str) -> int:
    """
    returns number of dash characters in an url
    """
    return url.count("-")


# Feature 10
def check_at_symbol(url: str) -> int:
    """
    check if '@' occurs in an url
    0 = yes
    1 = no
    """
    if "@" in url:
        return 1
    else:
        return 0


# Feature 11
def url_double_slash_count(url: str) -> int:
    """
    returns number of double slashes '//' in an url
    """
    return url.count("//")


# Feature 12
def subdomain_count(url: str) -> int:
    """
    returns number of subdomains in an url
    """
    subdomain = tldextract.extract(url).subdomain

    if len(subdomain) > 0:
        return (subdomain.strip()).count(".") + 1
    else:
        return 0


# Feature 13
def domain_dash_count(url: str) -> int:
    """
    returns number of dash chars in domain
    """
    extr = tldextract.extract(url)
    full_domain = ".".join(
        part for part in [
            extr.subdomain,
            extr.domain,
            extr.suffix
        ] if part)

    return (full_domain.strip()).count("-")


# Feature 14
def check_query(url: str) -> int:
    """
    checks if the url contains a query parameter (i.e. '?password=')
    """
    parsed_url = urlparse(url)

    if parsed_url.query:
        return 1
    else:
        return 0


# Feature 15
def calculate_ratio_of_digits(url: str) -> float:
    """
    calculates ratio of digits in an url
    """
    full_length = len(url)

    if full_length == 0:
        return 0.0

    # count digits
    numer_of_digits = sum(char.isdigit() for char in url.strip())

    # calc ratio
    ratio = numer_of_digits / full_length
    return ratio


# Feature 16
def check_rare_top_level_domain(url: str) -> int:
    """
    checks if the url has a rare top level domain by checking against from Public Suffix List (PSL)
    """
    extr = tldextract.extract(url)

    if extr.suffix:
        # known tld
        return 0
    else:
        # unknown tld
        return 1


# Feature 17
def url_non_alphanumeric_char_count(url: str) -> int:
    """
    returns number of non-alphanumeric characters in an url
    """
    not_alnum = 0

    for char in url:
        if not char.isalnum():
            not_alnum += 1

    return not_alnum


# Feature 18
def calculate_ratio_of_non_alphanumeric_chars(url: str) -> float:
    """
    calculates ratio of special chars in an url
    """
    full_length = len(url)

    if full_length == 0:
        return 0.0

    # count non-alphanumeric chars
    not_alnum = sum(not char.isalnum() for char in url)

    # calc ratio
    ratio = not_alnum / full_length
    return ratio


# Feature 19
def url_subdirectory_count(url: str) -> int:
    """
    returns number of subdirectories in an url
    """
    parsed = urlparse(url)
    path = parsed.path
    clean_path = path.strip('/')

    if not clean_path:
        return 0

    segments = clean_path.split('/')

    return len(segments)


# Feature 20
def url_query_param_count(url: str) -> int:
    """
    returns number of queries in an url
    """
    parsed = urlparse(url)
    query = parsed.query

    if not query:
        return 0

    segments = query.split('&')

    count = 0
    for segment in segments:
        if segment.strip():
            count += 1

    return count


# Feature 21
def domain_tld_length(url: str) -> int:
    """
    returns length of known Top Level Domain in an url
    """
    tld = tldextract.extract(url).suffix

    return len(tld)


# Feature 22
def check_anchor(url: str) -> int:
    """
    checks if the url contains a fragment identifier '#'
    """
    fragment = urlparse(url).fragment

    if fragment != "":
        return 1
    else:
        return 0


# Feature 23
def check_credentials(url: str) -> int:
    """
    checks if the url contains identifier for credentials
    """
    parsed = urlparse(url)

    # 1. check for credentials in netloc part (i.e. user:pass@domain.com)
    netloc = parsed.netloc
    if '@' in netloc and ':' in netloc.split('@')[0]:
        return 1

    # 2. check for credentials in query-strings
    query = parsed.query
    if query != "":
        qs_lower = query.lower()

        for keyword in CREDENTIAL_KEYWORDS:
            if f"{keyword}=" in qs_lower:
                return 1

    return 0


# Feature 24
def check_known_shortening_service(url: str) -> int:
    """
    checks if the url is from a well-known shortening service
    """
    netloc = urlparse(url).netloc

    if netloc in KNOWN_SHORTENING_SERVICES:
        return 1
    else:
        return 0


# Feature 25
def domain_char_count(url: str) -> int:
    """
    returns number of characters in a domain
    """
    netloc = urlparse(url).netloc
    return len(netloc.strip())


# Feature 26
def calculate_char_continuation_rate(url: str) -> float:
    """
    Calculates the CharContinuationRate of an URL.

    The rate is based on the sum of the lengths of the longest
    contiguous sequences of:
    1. Alphabetic characters (a-z, A-Z)
    2. Digits (0-9)
    3. Special characters (all other non-alphanumeric characters)

    This total length is divided by the overall length of the URL.
    """
    if not url:
        return 0.0

    # Helper function to find the length of the longest sequence for a given pattern
    def get_longest_sequence_length(pattern: str, text: str) -> int:
        sequences = re.findall(pattern, text)
        return len(max(sequences, key=len)) if sequences else 0

    # 1. Longest Alphabetic sequence length
    # Pattern: one or more letters [a-zA-Z]+
    longest_alphabet_len = get_longest_sequence_length(r'[a-zA-Z]+', url)

    # 2. Longest Digit sequence length
    # Pattern: one or more digits [0-9]+
    longest_digit_len = get_longest_sequence_length(r'[0-9]+', url)

    # 3. Longest Special Character sequence length
    # Pattern: one or more non-alphanumeric characters [^a-zA-Z0-9]+
    # This includes '.', '-', '/', ':', etc.
    longest_special_char_len = get_longest_sequence_length(r'[^a-zA-Z0-9]+', url)

    # 4. Total length of the longest sequences
    total_longest_length = longest_alphabet_len + longest_digit_len + longest_special_char_len

    # 5. Total length of the URL
    total_url_length = len(url)

    # 6. Calculate the Char Continuation Rate
    char_continuation_rate = total_longest_length / total_url_length

    return char_continuation_rate


# Feature 27
def check_suspicious_keywords(url: str) -> int:
    """
    checks if the url contains suspicious keywords from list above
    """
    for keyword in SUSPICIOUS_KEYWORDS:
        if keyword in url:
            return 1

    return 0


def extract_features(urls: List[str], label: int, csv_path: Path) -> pd.DataFrame:
    feature_funcs = {
        "url_length": url_char_count,
        "number_of_slashes": url_slash_count,
        "is_https": check_https,
        "number_of_http_occurrences": http_occurrence_count,
        "number_of_https_occurrences": https_occurrence_count,
        "number_of_dots": url_dot_count,
        "is_ip": check_ip,
        "number_of_digits": url_digit_count,
        "number_of_dashes": url_dash_count,
        "has_at_symbol": check_at_symbol,
        "number_of_double_slashes": url_double_slash_count,
        "number_of_subdomains": subdomain_count,
        "number_of_dashes_in_domain": domain_dash_count,
        "contains_query": check_query,
        "ratio_of_digits": calculate_ratio_of_digits,
        "rare_top_level_domain": check_rare_top_level_domain,
        "number_of_special_chars": url_non_alphanumeric_char_count,
        "ratio_of_special_chars": calculate_ratio_of_non_alphanumeric_chars,
        "number_of_subdirectories": url_subdirectory_count,
        "number_of_query_params": url_query_param_count,
        "tld_length": domain_tld_length,
        "has_fragment_identifier": check_anchor,
        "has_credentials": check_credentials,
        "is_known_shortening_service": check_known_shortening_service,
        "domain_length": domain_char_count,
        "char_continuation_rate": calculate_char_continuation_rate,
        "suspicious_keywords": check_suspicious_keywords,
    }

    records = [
        {
            "url": urllib.parse.unquote(u.strip()),
            "label": label,
            **{col: fn(urllib.parse.unquote(u.strip())) for col,
            fn in feature_funcs.items()}}
        for u in urls
    ]

    df = pd.DataFrame.from_records(records, columns=["url", "label", *feature_funcs.keys()])
    write_header = not csv_path.exists() or csv_path.stat().st_size == 0
    df.to_csv(
        csv_path,
        mode = "a",
        header = write_header,
        index = False,
        encoding = "utf-8",
    )

    return df


if __name__ == "__main__":
    # create argument parser
    parser = argparse.ArgumentParser(
        description="Extract Features from URLs for ML-Phishing-Detection-Algorithms"
    )

    # add arguments
    parser.add_argument(
        "--label",
        required=True,
        type=int,
        help="If CSV-File with URLs are Phishing set to 0, else set 1."
    )
    parser.add_argument(
        "--inputFile",
        required=True,
        help="Path to CSV-File with URLs to extract features from"
    )
    parser.add_argument(
        "--outputFile",
        required=True,
        help="Path to output CSV-File with extracted features"
    )

    # parse arguments
    args = parser.parse_args()

    # read csv
    print(f"\n🚀 Start extracting features from {args.inputFile} ...")
    urls = read_urls_from_csv(Path(args.inputFile))
    print(f"\n🧐 {len(urls)} urls found.")

    # set label and start feature extraction
    csv_path = Path('../data/features.csv')
    extract_features(urls, int(args.label), Path(args.outputFile))

    print(f"\n✏️ Features of {len(urls)} urls were extracted and written to {csv_path}")