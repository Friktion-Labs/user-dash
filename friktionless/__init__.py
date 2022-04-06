# module level doc-string
__doc__ = """
friktionless - a powerful Friktion data analysis library for Python
=====================================================================
**friktionless** is a Python package providing simplified interfaces to 
Friktion data. It aims to be the fundamental building block for doing 
data engineering and data analysis in Python for Friktion. Additionally, 
it has the broader goal of becoming **the most powerful and flexible open 
source data analysis / manipulation tool available for any protocol**. 

Main Features
-------------
Here are just a few of the things that friktionless does well:
  - Easy engineering of meaningful performance data about Friktion to enrich
    an analytical data warehouse.
  - Easy dashboarding and portal management for Friktion to manage the 
    growing body of analytics reporting and stakeholder needs.
  - Intuitive python first apis under the hood on everything - we're building 
    on top of pandas, altair, seaborn, and other popular python tools so the 
    community is enabled.
  - a Python and CLI api to enable data scientists and data engineers off the 
    same, singular codebase.


Example
------------
$ (friktion) johnreyes@pop-os:~/projects/user-dash$ python
>>> import friktionless
>>> q = friktionless.bitQuery.Queries()
>>> str = q.volt_pending_deposits("2022-03-01")
>>> print(str)


        {
        solana(network: solana) {
            transfers(
            transferType: {is: transfer}
            receiverAddress: {in: ["Hxtb6APfNtf9m8jJjh7uYp8fCTGr9aeHxBSfiPqCrV6G", "DA1M8mw7GnPNKU9ReANtHPQyuVzKZtsuuSbCyc2uX2du", "6asST5hurmxJ8uFvh7ZRWkrMfSEzjEAJ4DNR1is3G6eH", "FThcy5XXvab5u3jbA6NjWKdMNiCSV3oY5AAkvEvpa8wp", "7KqHFuUksvNhrWgoacKkqyp2RwfBNdypCYgK9nxD1d6K", "2P427N5sYcEXvZAZwqNzjXEHsBMESQoLyjNquTSmGPMb", "B3yakZxwomkmnCxRr8ZmQtiWgtxtVBuCREDFDdAvcCVQ", "A5MpyajTy6hdsg3S2em5ukcgY1ZBhxTxEKv8BgHajv1A", "BH7Jg3f97FyeGxsPR7FFskvfqGiaLeUnJ9Ksda53Jj8h", "5oV1Yf8q1oQgPYuHjepjmKFuaG2Wng9dzTqbSWhU5W2X", "A6XsYxGj9wpqUZG81XwgQJ2zJ3efCbuWSQfnkHqUSmdM", "wJAoeEG2sfQ1xgXUNVVkJ5mCTCw4SLc6oJafDwf6jTf","5LrULR5w9N1dfnJ9vHnzRkQi9uBL3CkSRWkDGTG6dP1e","2Ahpeqc1bo7Y4dVknvZUVfZPhHhym7JN6az2XMbUA6QA","FiEHDTKT6X7VFwGaUmsm1XXYr8vvkoSR5EqcY4znpefq"]}
            date: {is:"2022-03-01"}
            ) {
            amount
            currency {
                symbol
                name
            }
            date {
                date
            }
            transaction {
                signer
                success
                error
            }
            sender {
                address
            }
            block {
                timestamp {
                unixtime
                }
            }
            receiver {
                address
            }
            }
        }
        }


"""

import friktionless.friktion_etl
import friktionless.analytics
import friktionless.etl