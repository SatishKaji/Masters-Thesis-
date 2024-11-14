# Masters-Thesis
## Concept and Execution of an Analysis of Bibliographic Data in the OpenAlex Dataset using Apache Spark

This repository contains code for analyzing scholarly data from the OpenAlex dataset, focusing on publication trends, citations, authors, research fields, institutions, sources, and publishers. The analysis is performed using Apache Spark for distributed data processing and Python for scripting.

## Overview
The project leverages the OpenAlex dataset to provide insights into various aspects of scholarly publishing, including:

- **Publication Trends**: Analyzing trends in publication volume and distributions over time.
- **Citation and Self-Citation Analysis**: Examining citation patterns, including self-citations, to assess the impact of publications.
- **Author Analysis**: Evaluating author impact using metrics like the H-index and authorship trends.
- **Research Field Analysis**: Analyzing the distribution and growth of different research fields.
- **Publisher Analysis**: Analyzing publisher contributions.
- **Source Analysis**: Studying the top sources (journals, conferences, etc.) for publication frequency, trends, and impact.
- **Institution Analysis**: Analyzing institutional contributions to publications, identifying leading institutions, and tracking institutional growth over time.

## Key Features
- **Publication Analysis**: Identifies trends in publication volume, distributions, and key time periods.
- **Citation and Self-Citation Analysis**: Examines citation networks to assess the influence of works and track self-citations.
- **Author Impact**: Calculates H-index, authorship trends, and evaluates author impact.
- **Research Fields**: Analyzes the distribution of research across various fields and tracks field growth.
- **Publisher Analysis**: Assesses the number of publications per publisher and identifies key publishers.
- **Source Analysis**: Analyzes the publication trends across different sources (e.g., journals, conferences), identifying high-impact sources and their publication patterns.
- **Institution Analysis**: Examines institutional contributions to research, ranking institutions by publication count and tracking their growth.

## Technology Used
- **Apache Spark**: Distributed data processing for efficient handling of large datasets.
- **Python & PySpark**: Used for scripting and performing data analysis tasks.
- **OpenAlex API**: Retrieves and processes scholarly data from the OpenAlex dataset.

## File Structure
- **Analysis_of_publication.py**: Code for analyzing trends in publications.
- **Analysis_of_citations.py, Analysis_of_self_citations.py**: Code for analyzing citation and self-citation patterns.
- **Analysis_of_authors.py, Analysis_of_h_index.py**: Code for evaluating author-level metrics like H-index and authorship.
- **Analysis_of_research_fields.py**: Code for analyzing trends in different research fields.
- **Analysis_of_publishers.py**: Code for analyzing publisher contributions and trends.
- **Analysis_of_sources.py**: Code for analyzing the impact of different publication sources (e.g., journals, conferences).
- **Analysis_of_institutions.py**: Code for analyzing institutional contributions and growth.
- **Master's Thesis Report.pdf**: Full report summarizing the findings from the analysis of publications, citations, authors, research fields, publishers, sources, and institutions.
