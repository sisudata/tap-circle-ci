import requests
import singer
import singer.metrics as metrics


session = requests.Session()
logger = singer.get_logger()


def add_authorization_header(token: str) -> None:
    """
    Adds authorization header
    """
    session.headers.update({'Circle-Token': token})


def add_next_page_to_url(url: str, next_page_token: str) -> str:
    """
    Adds token to header to pull next page
    """
    return url + '?page-token=' + next_page_token


def get(source: str, url: str, headers: dict={}):
    """
    Get a single page from the provided url
    """
    with metrics.http_request_timer(source) as timer:
        session.headers.update(headers)
        resp = session.request(method='get', url=url)
        if resp.status_code == 401:
            raise AuthException(resp.text)
        if resp.status_code == 403:
            raise AuthException(resp.text)
        if resp.status_code == 404:
            raise NotFoundException(resp.text)

        timer.tags[metrics.Tag.http_status_code] = resp.status_code
        return resp


def get_all_pages(source: str, url: str, headers: dict={}):
    temp_url = str(url)
    while True:
        r = get(source, temp_url, headers)
        r.raise_for_status()
        data = r.json()
        yield data
        if data.get('next_page_token') is not None:
            temp_url = add_next_page_to_url(url, data.get('next_page_token'))
        else:
            session.headers.pop("next_page_token", None)
            break


def get_all_items(source: str, url: str, headers: dict={}):
    """
    Each page contains a bunch of items, so this function extracts the items one by one
    """
    for page in get_all_pages(source, url, headers):
        for item in page["items"]:
            yield item
