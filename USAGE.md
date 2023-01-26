# USAGE

- [USAGE](#usage)
  - [Main class](#main-class)
  - [Notebook with existing code (pyspark-template module)](#notebook-with-existing-code-pyspark-template-module)
  - [Go big data ?](#go-big-data-)
  - [SQL Test](#sql-test)
    - [Query 1](#query-1)
    - [Query 2](#query-2)


## Main class

* The main class is defined in `pyproject.toml` with [Click](https://click.palletsprojects.com/en/8.1.x/) with:
```
[tool.poetry.scripts]
drugs_gen = "pyspark_template.jobs.drugs_gen:drugs_gen"
```
* Setup with [README.md](README.md)
* `make dev` will build the `data/output/result.json` by default with the data from `data` folder
* We could specify differents path for each csv date file `poetry run drugs_gen --help`

```json
[
    {
        "atccode": "A01AD",
        "drug": "EPINEPHRINE",
        "pubmeds": [
            {
                "pubmed_id": "8",
                "pubmed_title": "Time to epinephrine treatment morality is associated with the risk of mortality in children who achieve sustained ROSC after traumatic out-of-hospital cardiac arrest.",
                "pubmed_date": "01/03/2020",
                "pubmed_journal": "The journal of allergy and clinical immunology. In practice"
            },
            {
                "pubmed_id": "7",
                "pubmed_title": "The High Cost of Epinephrine Self Autoinjectors and Possible Alternatives.",
                "pubmed_date": "01/02/2020",
                "pubmed_journal": "The journal of allergy and clinical immunology. In practice"
            }
        ],
        "clinical_trials": [
            {
                "clinical_trials_id": "NCT04188185",
                "clinical_trials_scientific_title": "Corana nphung Epinephrine During Exploratory Tympanotomy",
                "clinical_trials_date": "27 April 2020",
                "clinical_trials_journal": "Journal of emergency nursing"
            },
            {
                "clinical_trials_id": "NCT04188184",
                "clinical_trials_scientific_title": "Tranexamic Acid nphung Epinephrine During Exploratory Tympanotomy",
                "clinical_trials_date": "27 April 2020",
                "clinical_trials_journal": "Journal of emergency nursing\\xc3\\x28"
            }
        ],
        "journals": [
            {
                "date": "01/02/2020",
                "journal": "The journal of allergy and clinical immunology. In practice"
            },
            {
                "date": "01/03/2020",
                "journal": "The journal of allergy and clinical immunology. In practice"
            },
            {
                "date": "27 April 2020",
                "journal": "Journal of emergency nursing"
            },
            {
                "date": "27 April 2020",
                "journal": "Journal of emergency nursing\\xc3\\x28"
            }
        ]
    },
    {
        "atccode": "S03AA",
        "drug": "TETRACYCLINE",
        "pubmeds": [
            {
                "pubmed_id": "6",
                "pubmed_title": "Rapid reacquisition of contextual fear following extinction in mice: effects of amount of extinction, tetracycline acute ethanol withdrawal, and ethanol intoxication.",
                "pubmed_date": "2020-01-01",
                "pubmed_journal": "Psychopharmacology"
            },
            {
                "pubmed_id": "5",
                "pubmed_title": "Appositional Tetracycline bone formation rates in the Beagle Beatle.",
                "pubmed_date": "02/01/2020",
                "pubmed_journal": "American journal of veterinary research"
            },
            {
                "pubmed_id": "4",
                "pubmed_title": "Tetracycline Resistance Patterns of Lactobacillus buchneri 123 Group Strains.",
                "pubmed_date": "01/01/2020",
                "pubmed_journal": "Journal of food protection"
            }
        ],
        "clinical_trials": [
            {}
        ],
        "journals": [
            {
                "date": "01/01/2020",
                "journal": "Journal of food protection"
            },
            {
                "date": "2020-01-01",
                "journal": "Psychopharmacology"
            },
            {
                "date": "02/01/2020",
                "journal": "American journal of veterinary research"
            },
            {}
        ]
    },
    {
        "atccode": "V03AB",
        "drug": "ETHANOL",
        "pubmeds": [
            {
                "pubmed_id": "6",
                "pubmed_title": "Rapid reacquisition of contextual fear following extinction in mice: effects of amount of extinction, tetracycline acute ethanol withdrawal, and ethanol intoxication.",
                "pubmed_date": "2020-01-01",
                "pubmed_journal": "Psychopharmacology"
            }
        ],
        "clinical_trials": [
            {}
        ],
        "journals": [
            {
                "date": "2020-01-01",
                "journal": "Psychopharmacology"
            },
            {}
        ]
    },
    {
        "atccode": "A04AD",
        "drug": "DIPHENHYDRAMINE",
        "pubmeds": [
            {
                "pubmed_id": "3",
                "pubmed_title": "Diphenhydramine hydrochloride helps symptoms of ciguatera fish poisoning.",
                "pubmed_date": "02/01/2019",
                "pubmed_journal": "The Journal of pediatrics"
            },
            {
                "pubmed_id": "1",
                "pubmed_title": "A 65-year-old man with erythema of the face diphenhydramine, neck, and chest, weakness, nphung and palpitations",
                "pubmed_date": "01/01/2019",
                "pubmed_journal": "Journal of emergency nursing"
            },
            {
                "pubmed_id": "2",
                "pubmed_title": "An evaluation of benadryl, pyribenzamine, and other so-called diphenhydramine antihistaminic drugs in the treatment of allergy.",
                "pubmed_date": "01/01/2019",
                "pubmed_journal": "Journal of emergency nursing"
            }
        ],
        "clinical_trials": [
            {
                "clinical_trials_id": "NCT01967433",
                "clinical_trials_scientific_title": "Use of Diphenhydramine as an Adjunctive Sedative nphung for Colonoscopy in Patients Chronically on Opioids",
                "clinical_trials_date": "1 January 2020",
                "clinical_trials_journal": "Journal of emergency nursing"
            },
            {
                "clinical_trials_id": "NCT04189588",
                "clinical_trials_scientific_title": "Phase 2 Study IV QUZYTTIR\u2122 (Cetirizine Hydrochloride Injection) vs V Diphenhydramine",
                "clinical_trials_date": "1 January 2020",
                "clinical_trials_journal": "Journal of emergency nursing"
            },
            {
                "clinical_trials_id": "NCT04237091",
                "clinical_trials_scientific_title": "Feasibility of a Randomized Controlled Clinical Trial Comparing the Use of Cetirizine to Replace Diphenhydramine in the Prevention of Reactions Related to Paclitaxel",
                "clinical_trials_date": "1 January 2020",
                "clinical_trials_journal": "Journal of emergency nursing"
            }
        ],
        "journals": [
            {
                "date": "01/01/2019",
                "journal": "Journal of emergency nursing"
            },
            {
                "date": "02/01/2019",
                "journal": "The Journal of pediatrics"
            },
            {
                "date": "1 January 2020",
                "journal": "Journal of emergency nursing"
            }
        ]
    },
    {
        "atccode": "A03BA",
        "drug": "ATROPINE",
        "pubmeds": [
            {}
        ],
        "clinical_trials": [
            {}
        ],
        "journals": [
            {}
        ]
    },
    {
        "atccode": "6302001",
        "drug": "ISOPRENALINE",
        "pubmeds": [
            {}
        ],
        "clinical_trials": [
            {}
        ],
        "journals": [
            {}
        ]
    },
    {
        "atccode": "R01AD",
        "drug": "BETAMETHASONE",
        "pubmeds": [
            {}
        ],
        "clinical_trials": [
            {
                "clinical_trials_id": "NCT04153396",
                "clinical_trials_scientific_title": "Preemptive 1234 With Betamethasone and Ropivacaine for Postoperative Pain in Laminoplasty or \\xc3\\xb1 Laminectomy",
                "clinical_trials_date": "1 January 2020",
                "clinical_trials_journal": "H\u00f4pitaux Universitaires de Gen\u00e8ve"
            }
        ],
        "journals": [
            {},
            {
                "date": "1 January 2020",
                "journal": "H\u00f4pitaux Universitaires de Gen\u00e8ve"
            }
        ]
    },
    {
        "atccode": "HELLO",
        "drug": "CORONA",
        "pubmeds": [
            {}
        ],
        "clinical_trials": [
            {}
        ],
        "journals": [
            {}
        ]
    }
]
```

I consider this project a minimal to start a project, but I think maybe we could add this too:
  * Use `Google Style Python Docstrings` 
  * Force `mypy`
  * Setup `tox/nox` if we need to manage several python versions
  * Store constants / Spark Schema (Struct) in a schemas python submodule
  * Setup CI with `Jenkins` or `Github/Gitlab Actions`, etc...
  * A contributing guide like [Angular Contributing Guide](https://github.com/angular/angular/blob/main/CONTRIBUTING.md) 
  * Generate automatically Changelog with something like [keepachangelog](https://keepachangelog.com/en/1.0.0/)
  * Add target distributed environment for Spark (Kubernetes, YARN, Google Cloud, AWS, Azure, etc...)
  * Manage projects releases and versionning automatically with CICD tools
  * Generate code coverage and/or integrate with Sonar for Python in addition to flake8
  * Add a deeper tests strategies

## Notebook with existing code (pyspark-template module)

* Notebook usage sample with [adhoc.iypnb](notebooks/adhoc.ipynb), it's using the `data/output/result.json`  from [Main class](#main-class)  **result**

| journal                                                     | numberOfDrugs |
| ----------------------------------------------------------- | ------------- |
| The journal of allergy and clinical immunology. In practice | 2             |
| Psychopharmacology                                          | 2             |
| Journal of emergency nursing                                | 2             |
| Journal of food protection                                  | 1             |
| Hôpitaux Universitaires de Genève                           | 1             |
| Journal of emergency nursing\xc3\x28                        | 1             |
| American journal of veterinary research                     | 1             |
| The Journal of pediatrics                                   | 1             |

## Go big data ?

We could:
 
 * Use input/output BigData format like Parquet, ORC, Hudi, Iceberg or Delta lake

```
What is Parquet? Apache Parquet is an open source, column-oriented data file format designed for efficient data storage and retrieval. It provides efficient data compression and encoding schemes with enhanced performance to handle complex data in bulk.
```

* Pyspark is used here for big data purpose. We could choose another distributed python framework like Dask or Rapids If we have GPU hardware available. 

* Have a distributed infrastructure On-Premise or Cloud with a cluster of workers 

* Input format ideas:
  * For analytics/machine learning use case: we could use bigdata output format (Parquet, ORC, Hudi, Iceberg ou Delta lake)
  * For web API: a NoSQL document database like MongoDB
  * For search API: a ElasticSearch for search queries with the data (index, search & aggregation)

## SQL Test
* The notebook [sql.ipynb](notebooks/sql.ipynb) was used to build SQL compatible Spark

### Query 1

```sql
SELECT date, sum(prod_price*prod_qty) as ventes FROM product_nomenclature 
    INNER JOIN transaction ON transaction.prod_id=product_nomenclature.product_id
    WHERE date between '01/01/2019' and '31/12/2019'
    GROUP BY date
    ORDER BY date
```

### Query 2

```sql
SELECT client_id, 
        sum(case when product_type="MEUBLE" then ventes end) as ventes_meuble,
        sum(case when product_type="DECO" then ventes end) as ventes_deco
    FROM (
    SELECT client_id,product_type, SUM(prod_price*prod_qty) as ventes  FROM product_nomenclature 
    INNER JOIN transaction ON transaction.prod_id=product_nomenclature.product_id
    WHERE date between '01/01/2019' and '31/12/2019'
    GROUP BY client_id, product_type
) GROUP BY client_id
```



