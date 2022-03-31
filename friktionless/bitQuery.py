from isort import api
import requests

# BitQuery API Key -- eventually should probably be set as an environment variable on the box which hosts the ETL script
api_key = 'BQYCaXaMZlqZrPCSQVsiJrKtxKRVcSe4'

class Queries:
    
    # BitQuery query string which returns pending deposit transfers from Friktion users to specific Friktion Volts
    def volt_pending_deposits(self, str_dt):

        query = '''

        {
        solana(network: solana) {
            transfers(
            transferType: {is: transfer}
            receiverAddress: {in: ["Hxtb6APfNtf9m8jJjh7uYp8fCTGr9aeHxBSfiPqCrV6G", "DA1M8mw7GnPNKU9ReANtHPQyuVzKZtsuuSbCyc2uX2du", "6asST5hurmxJ8uFvh7ZRWkrMfSEzjEAJ4DNR1is3G6eH", "FThcy5XXvab5u3jbA6NjWKdMNiCSV3oY5AAkvEvpa8wp", "7KqHFuUksvNhrWgoacKkqyp2RwfBNdypCYgK9nxD1d6K", "2P427N5sYcEXvZAZwqNzjXEHsBMESQoLyjNquTSmGPMb", "B3yakZxwomkmnCxRr8ZmQtiWgtxtVBuCREDFDdAvcCVQ", "A5MpyajTy6hdsg3S2em5ukcgY1ZBhxTxEKv8BgHajv1A", "BH7Jg3f97FyeGxsPR7FFskvfqGiaLeUnJ9Ksda53Jj8h", "5oV1Yf8q1oQgPYuHjepjmKFuaG2Wng9dzTqbSWhU5W2X", "A6XsYxGj9wpqUZG81XwgQJ2zJ3efCbuWSQfnkHqUSmdM", "wJAoeEG2sfQ1xgXUNVVkJ5mCTCw4SLc6oJafDwf6jTf","5LrULR5w9N1dfnJ9vHnzRkQi9uBL3CkSRWkDGTG6dP1e","2Ahpeqc1bo7Y4dVknvZUVfZPhHhym7JN6az2XMbUA6QA","FiEHDTKT6X7VFwGaUmsm1XXYr8vvkoSR5EqcY4znpefq"]}
            date: {is:'''+'"'+ str_dt +'"'+ '''}
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

        '''

        return query


def run_query(query):  # A simple function to use requests.post to make the API call.
    
    headers = {'X-API-KEY': api_key}
    request = requests.post('https://graphql.bitquery.io/', json={'query': query}, headers=headers)
    if request.status_code == 200:
        return request.json()
    else:
        print(request.reason)
        raise Exception('Query failed and return code is {}.{}'.format(request.status_code, query))