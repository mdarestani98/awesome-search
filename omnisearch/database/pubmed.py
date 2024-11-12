import logging
from typing import List, Union

import pandas as pd
from Bio import Entrez

from ..utils.decorators import disk_cache_results
from ..utils.xml_tools import safe_parse
from .base_database import DatabaseAPI


logger = logging.getLogger(__name__)
if not logger.hasHandlers():
    logging.basicConfig(level=logging.DEBUG,
                        format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
                        handlers=[logging.StreamHandler()]
                        )


class PubMed(DatabaseAPI):
    def __init__(self, api_key: str, email: str):
        super().__init__(api_key)
        self.email = email
        Entrez.email = email
        Entrez.api_key = api_key
        self.logger = logger
        logger.info(f"PubMed API initialized with email: {email}")

    @disk_cache_results
    def search(self, query: str, count: int, batch_size: int = 100, force: bool = False):
        logger.debug(f"Searching PubMed for: {query}")
        try:
            handle = Entrez.esearch(db='pubmed', term=query, retmax=0)
            record = Entrez.read(handle)
            total_count = min(int(record['Count']), count) if count > 0 else int(record['Count'])
            handle.close()

            if total_count == 0:
                logger.warning(f"No results found for the given query")
                return None
            logger.info(f"Total articles found: {total_count}")

            id_list = []
            for start in range(0, total_count, batch_size):
                logger.debug(f"Fetching IDs from {start + 1} to {min(start + batch_size, total_count)}")
                handle = Entrez.esearch(db='pubmed', term=query, retstart=start, retmax=batch_size)
                record = Entrez.read(handle)
                id_list.extend(record['IdList'])
                handle.close()
                logger.debug(f"Fetched {len(id_list)} IDs")

            logger.info(f"Total IDs fetched: {len(id_list)}")
            return id_list

        except Exception as e:
            logger.error(f"Error searching PubMed: {e}", exc_info=True)
            return None

    @disk_cache_results
    def fetch(self, id_list: List[str], batch_size: int = 100, force: bool = False):
        logger.debug(f"Fetching articles for {len(id_list)} IDs")
        all_records = []
        try:
            for start in range(0, len(id_list), batch_size):
                batch_ids = id_list[start:start + batch_size]
                logger.debug(f"Fetching articles from {start + 1} to {min(start + batch_size, len(id_list))}")
                handle = Entrez.efetch(db='pubmed', id=','.join(batch_ids), retmode='text', rettype='xml')
                records = Entrez.read(handle)['PubmedArticle']
                all_records.extend(records)
                handle.close()
                logger.debug(f"Fetched {len(all_records)} articles")

            logger.info(f"Total articles fetched: {len(all_records)}")
            return all_records

        except Exception as e:
            logger.error(f"Error fetching articles from PubMed: {e}", exc_info=True)
            return None

    def parse(self, article_list) -> Union[pd.DataFrame, None]:
        logger.debug(f"Parsing {len(article_list)} articles")
        records = []
        try:
            for article in article_list:
                title = safe_parse(article, ('MedlineCitation', 'Article', 'ArticleTitle'))
                abstract = safe_parse(article, ('MedlineCitation', 'Article', 'Abstract', 'AbstractText'))
                authors_list = safe_parse(article, ('MedlineCitation', 'Article', 'AuthorList'))
                authors = ", ".join([
                    " ".join(filter(None, [author.get("ForeName", ""), author.get("LastName", "")]))
                    for author in authors_list
                ])
                pub_date = safe_parse(article, ('MedlineCitation', 'Article', 'Journal', 'JournalIssue', 'PubDate'))
                if "Year" in pub_date:
                    pub_year = pub_date["Year"]
                elif "MedlineDate" in pub_date:
                    pub_year = pub_date["MedlineDate"].split()[0]
                else:
                    pub_year = None
                source = safe_parse(article, ('MedlineCitation', 'Article', 'Journal', 'Title'))
                pub_types = safe_parse(article, ('MedlineCitation', 'Article', 'PublicationTypeList'))
                pub_type = "; ".join(pub_types)
                doi = None
                for id_info in safe_parse(article, ('PubmedData', 'ArticleIdList')):
                    if id_info.attributes['IdType'] == 'doi':
                        doi = str(id_info)
                        break
                keywords = safe_parse(article, ('MedlineCitation', 'KeywordList'))
                if isinstance(keywords, list):
                    keywords = "; ".join([keyword for sublist in keywords for keyword in sublist])
                else:
                    keywords = None
                mesh_terms = safe_parse(article, ('MedlineCitation', 'MeshHeadingList'))
                if isinstance(mesh_terms, list):
                    mesh = "; ".join([mesh["DescriptorName"] for mesh in mesh_terms])
                else:
                    mesh = None
                article_type = safe_parse(article, ('MedlineCitation', 'Article', 'ArticleType'))

                records.append({
                    "title": title,
                    "abstract": abstract,
                    "authors": authors,
                    "pub_year": pub_year,
                    "source": source,
                    "pub_type": pub_type,
                    "doi": doi,
                    "keywords": keywords,
                    "mesh": mesh,
                    "article_type": article_type
                })
                logger.debug(f"Parsed {len(records)} articles")

            df = pd.DataFrame(records)
            logger.info(f"Total articles parsed: {len(df)}")
            return df

        except Exception as e:
            logger.error(f"Error parsing articles: {e}", exc_info=True)
            return None

    def generate_query(self, query: str):
        pass


if __name__ == '__main__':
    query = ('(exercise OR "physical activity" OR cardio OR aerobic OR endurance OR resistance OR strength OR workout '
             'OR "High-intensity interval training" OR exergaming OR sedentary OR "sedentary behaviour" OR activity '
             'OR "screen time" OR "sitting sport" OR "exercise training" OR "physical activit*") '
             'AND (Attention AND deficit AND hyperactivity) OR ADHD OR "attention deficit disorder with hyperactivity" '
             'OR "attention deficit disorder*" OR "hyperkinetic disorder*" OR "hyperkinetic syndrome*") '
             'AND ("meta- analy*" OR meta-analysis)')
    from dotenv import load_dotenv
    import os
    load_dotenv()
    pubmed = PubMed(api_key=os.getenv('NCBI_API_KEY'), email=os.getenv('NCBI_EMAIL'))
    id_list = pubmed.search(query, count=200)
    data = pubmed.fetch(id_list)
    df = pubmed.parse(data)
    print(df.head())

