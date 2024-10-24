# pgnc_upload

This script uploads a TSV file to the PGNC database. It parses the input file, processes the data,
and inserts it into the database using an SSH tunnel through a bastian host.

The TSV file must contain the following columns:
- PotriID
- Gene symbol
- Symbol type (value must be approved, alias or previous)
- Gene name
- Name type (value must be approved, alias or previous)
- Location
- Locus type

All the fieild will be uploaded and the status will be set to 'internal' so that uploads don't
appear live for data safety reasons.

```TSV
PotriID	Gene symbol	Symbol type	Gene name	Name type	Location	Locus type
Potri.001G068400	CYP51G1	approved	cytochrome P450 family 51 subfamily G member 1	approved	1	protein-coding gene
```

The TSV file can be placed in the ./data/tsv directory of the project.

## Prerequisites

- A copy of the PGNC database that is sitting in a VPC that is accessible through a bastion host
  (see https://github.com/HGNC/pgnc_db_schema).
- Python 3 (code was written using 3.13).
- Pip package manager.
- An RSA private key file named **pgnc.pem** that will allow you access to the bastion host.
- A .env file containing the following fields:
    * DYLD_FALLBACK_LIBRARY_PATH=*`libssl.3.dylib & libpq.5.dylib parent directory`*
    * PYTHONPATH=*`Path to project`*/pgnc_upload/src
    * PKEY_NAME=pgnc.pem
    * BASTION_IP=*`IP of Bastion`*
    * BASTION_USER=*`Bastion username`*
    * DB_NAME=*`DB name`*
    * DB_USER=*`DB username`*
    * DB_PASS=*`DB password`*
    * RDS_HOST=*`DB endpoint`*

## Installation

First create a virtual environment.

```bash
python3 -m venv .venv
source .venv/bin/activate
```

Now that you have a virtual environment you can then install the required python packages using
the requirements.txt file

```bash
pip install -r requirements
```

Create a **.env** file as stated above in the project's root directory and place your **pgnc.pem**
file in the same place.

To make sure the script will work for you, run the unit tests.

```bash
python -m unittest tests/*.py
```

If all is well, you can run the **main.py** script in the **src** directory or build an executable (see below).

### Build an executable (Optional)

In the projects root directory run the following command which will create an executable file named
**pgnc_upload** in the **dist** director:

```bash
pyinstaller --name pgnc_upload src/main.py
```

## Usage

To show the scripts help run:

```bash
python3 ./main.py --help
```

To run over a TSV file in the data directory of the project, do the following:

```bash
python3 ./main.py --file ../data/tsv/pgnc_upload.tsv
```

If you have built the executable, change into the **dist** directory and instead of calling
`python3 ./main.py --help` to run the code, use `./pgnc_upload --help` etc.

## License

[GNU Affero General Public License v3.0](/LICENSE)