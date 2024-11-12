import logging

import pandas as pd

from ..utils.decorators import disk_cache_results
from .base_database import DatabaseAPI

logger = logging.getLogger(__name__)
if not logger.hasHandlers():
    logging.basicConfig(level=logging.DEBUG,
                        format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
                        handlers=[logging.StreamHandler()]
                        )


class Scopus(DatabaseAPI):
    def __init__(self, api_key: str):
        super().__init__(api_key)
        from pybliometrics import scopus
        scopus.init(keys=[self.api_key])
        self.logger = logger
        logger.info(f"Scopus API initialized")

    @disk_cache_results
    def search(self, query: str, force: bool = False):
        logger.debug(f"Searching Scopus for: {query}")
        try:
            from pybliometrics import scopus
            results = scopus.ScopusSearch(query, refresh=force, view='COMPLETE')
            logger.info(f"Total articles found: {results.get_results_size()}")
            return results
        except Exception as e:
            logger.error(f"Error searching Scopus: {e}", exc_info=True)
            return None

    def parse(self, data) -> pd.DataFrame:
        if data is None:
            return pd.DataFrame()

        if not isinstance(data, list):
            data = data.results
        logger.debug(f"Parsing {len(data)} articles")

        results = []
        for article in data:
            title = article.title
            authors = article.author_names
            pub_year = article.coverDate.split("-")[0] if article.coverDate else None
            abstract = article.description
            source = article.publicationName
            doc_type = article.subtypeDescription
            pub_type = article.aggregationType
            citations = article.citedby_count
            doi = article.doi
            link = f'https://www.scopus.com/record/display.uri?eid={article.eid}'
            keywords = article.authkeywords

            results.append({
                "title": title,
                "abstract": abstract,
                "authors": authors,
                "pub_year": pub_year,
                "source": source,
                "pub_type": pub_type,
                "doi": doi,
                "keywords": keywords,
                "doc_type": doc_type,
                "citations": citations,
                "link": link
            })
            logger.debug(f"Parsed: {len(results)} articles")

        df = pd.DataFrame(results)
        logger.info(f"Total articles parsed: {len(df)}")
        return df

    def generate_query(self, query: str):
        pass


if __name__ == '__main__':
    from dotenv import load_dotenv
    import os
    load_dotenv()
    elsevier_api_key = os.getenv('ELSEVIER_API_KEY')
    scopus = Scopus(api_key=elsevier_api_key)
    query = 'TITLE-ABS-KEY ( "machine learning"  AND  "robot*"  AND  "inspection"  AND  "building" )'
    records = scopus.search(query)
    df = scopus.parse(records)
    print(df.head())

