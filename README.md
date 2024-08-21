# SignON Extractor
The SignON Extractor repository is part of [SignON](https://signon-project.eu/) an EU H2020 research and innovation funded project.
The SignON Extractor is a tool with the aim of filtering and downloading contribution files collected during the SIgnON Project utilising the [SignOn ML App](https://github.com/signon-project/wp2-ml-app).

### Usage
- Modify Placeholder parameters in config.yml file
- python download\_contributions.py [-h] [-p PHONENUMBERHASH] [-a AGE]

[-al ANNOTATIONLANGUAGE] [-g GENDER]

[-lt LANGUAGETYPE] [-mt MESSAGETYPE]

[-r REGISTER] [-sl SOURCELANGUAGE]

[-hs HEARINGSTATUS] [-dest DESTINATION]

[-usr USERNAME] [-pwd PASSWORD]

[-ps PLAINSTRUCTURE] [-uz UNZIP] [-all ALL]

Arguments for extracting contribution files from Minio

optional arguments:

- h, --help            show this help message and exit
- p PHONENUMBERHASH, --phoneNumberHash PHONENUMBERHASH

hash-phone-number from which download the files

(default: all hash-phone-number) (default: None)

- a AGE, --age AGE     Metadata Age (default: None)
- al ANNOTATIONLANGUAGE, --annotationLanguage ANNOTATIONLANGUAGE

Metadata Annotation Language (default: None)

- g GENDER, --gender GENDER

Metadata Gender (default: None)

- lt LANGUAGETYPE, --languageType LANGUAGETYPE

Metadata Language Type (default: None)

- mt MESSAGETYPE, --messageType MESSAGETYPE

Metadata Message Type (default: None)

- r REGISTER, --register REGISTER

Metadata Register (default: None)

- sl SOURCELANGUAGE, --sourceLanguage SOURCELANGUAGE

Metadata Source Language (default: None)

- hs HEARINGSTATUS, --hearingStatus HEARINGSTATUS

Metadata Hearing Status (default: None)

- dest DESTINATION, --destination DESTINATION

Destination folder (default: minioDownload)

- usr USERNAME, --username USERNAME

Username for minio access (default: None)

- pwd PASSWORD, --password PASSWORD

Password for minio access (default: None)

- ps PLAINSTRUCTURE, --plainStructure PLAINSTRUCTURE

Flag to define if download file should be grouped by

user or not (default: False)

- uz UNZIP, --unzip UNZIP

Flag to define if zip files should be automatically

unzipped after download (default: False)

- all ALL, --all ALL   Download all files not filtering for metadata

(default: False)

## Authors
This project was developed by [FINCONS GROUP AG](https://www.finconsgroup.com/) within the Horizon 2020 European project SignON under grant agreement no. [101017255](https://doi.org/10.3030/101017255).  
For any further information, please send an email to [signon-dev@finconsgroup.com](mailto:signon-dev@finconsgroup.com).

## License
This project is released under the [Apache 2.0 license](https://www.apache.org/licenses/LICENSE-2.0.html).