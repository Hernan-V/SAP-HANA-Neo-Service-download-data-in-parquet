# Download table in several files grouped by specific fields

We will download the table __T001__ from a SAP ECC database.
<details>

<summary>Previous analysis of the data</summary>
First we will analyze a sample of data to understand the fields of the table executing this SQL directly on the database:

```sql
select top 20 * FROM ECC.T001
```

As a result we will get something like these sample:
| MANDT | BUKRS | BUTXT                     | ORT01                     | LAND1 | WAERS | SPRAS | KTOPL | WAABW | PERIV | KOKFI | RCOMP | ADRNR      | STCEG        | FIKRS | XFMCO | XFMCB | XFMCA | TXJCD | FMHRDATE | BUVAR | FDBUK | XFDIS | XVALV | XSKFN | KKBER | XMWSN | MREGL | XGSBE | XGJRV | XKDFT | XPROD | XEINK | XJVAA | XVVWA | XSLTA | XFDMM | XFDSD | XEXTB | EBUKR | KTOP2 | UMKRS | BUKRS_GLOB | FSTVA | OPVAR | XCOVR | TXKRS | WFVAR | XBBBF | XBBBE | XBBBA | XBBKO | XSTDT | MWSKV | MWSKA | IMPDA | XNEGP | XKKBI | WT_NEWWT | PP_PDATE | INFMT | FSTVARE | KOPIM | DKWEG | OFFSACCT | BAPOVAR | XCOS | XCESSION | XSPLT | SURCCM | DTPROV | DTAMTC | DTTAXC | DTTDSP | DTAXR | XVATDATE | PST_PER_VAR | FM_DERIVE_ACC | XBBSC |
|-------|-------|---------------------------|---------------------------|-------|-------|-------|-------|-------|-------|-------|-------|------------|--------------|-------|-------|-------|-------|-------|----------|-------|-------|-------|-------|-------|-------|-------|-------|-------|-------|-------|-------|-------|-------|-------|-------|-------|-------|-------|-------|-------|-------|------------|-------|-------|-------|-------|-------|-------|-------|-------|-------|-------|-------|-------|-------|-------|-------|----------|----------|-------|---------|-------|-------|----------|---------|------|----------|-------|--------|--------|--------|--------|--------|-------|----------|-------------|---------------|-------|
| 000   | 0001  | SAP A.G.                  | Walldorf                  | DE    | EUR   | D     | INT   | 10    | K4    | 2     |       |            |              |       |       |       |       |       | 00000000 |       |       |       |       |       | 0001  |       |       |       | X     |       |       |       |       | X     |       |       |       |       |       |       | 0001  |            | 0001  | 0001  |       |       | 0001  |       |       |       |       |       | V0    | A0    | 1     |       | X     |          |          |       | FMRE    |       |       | 0        |         |      |          |       |        |        |        |        |        |       |          |             |               |       |
| 000   | 0003  | SAP US (IS-HT-SW)         | Palo Alto                 | US    | USD   | E     | INT   | 10    | K4    | 1     |       |            |              |       |       |       |       |       | 00000000 |       |       |       | X     |       | AMER  |       |       |       |       |       |       |       |       |       |       |       |       |       |       |       | 0001  |            | 0001  | 0001  |       |       | 0001  |       |       |       |       |       | V0    | A0    | 1     |       |       |          |          |       | FMRE    |       |       | 0        |         |      |          |       |        |        |        |        |        |       |          |             |               |       |
| 000   | 0MB1  | IS-B Musterbank Deutschl. | Walldorf                  | DE    | EUR   | D     | 0MB1  | 00    | K4    | 1     |       |            |              |       |       |       |       |       | 00000000 |       |       |       |       |       |       |       |       |       |       |       |       |       |       |       |       |       |       |       |       |       |       |            | 0001  | 0001  |       |       |       |       |       |       |       |       |       |       |       |       |       |          |          |       |         |       |       | 0        |         |      |          |       |        |        |        |        |        |       |          |             |               |       |
| 000   | AR01  | Country Template AR       | Argentinien               | AR    | ARS   | S     | INT   | 00    | K4    |       |       |            |              |       |       |       |       |       | 00000000 | 2     |       |       | X     |       |       |       |       |       | X     |       |       |       |       |       |       |       |       |       |       |       |       |            | 0001  | 0001  |       |       |       |       |       |       |       |       | C0    | D0    |       |       |       | X        |          |       |         |       |       | 0        |         |      |          |       |        |        |        |        |        |       |          |             |               |       |
| 000   | ARG1  | Country Template AR       | Argentinien               | AR    | ARS   | S     | INT   | 10    | K4    |       |       |            |              |       |       |       |       |       | 00000000 | 2     |       |       | X     |       |       |       |       |       | X     |       |       |       |       |       |       |       |       |       |       |       |       |            | 0001  | 0001  |       |       |       |       |       |       |       |       |       |       |       |       |       |          |          |       |         |       |       | 0        |         |      |          |       |        |        |        |        |        |       |          |             |               |       |
| 000   | AT01  | Country Template AT       | Austria                   | AT    | EUR   | D     | INT   | 10    | K4    | 2     |       |            |              |       |       |       |       |       | 00000000 | 1     |       |       |       |       |       |       |       | X     | X     |       |       |       |       | X     |       |       |       |       |       |       |       |            | 0001  | 0001  |       |       |       |       |       |       |       |       | V0    | A0    |       |       |       |          |          |       |         |       |       | 0        |         |      | X        |       |        |        |        |        |        |       |          |             |               |       |
| 000   | AU01  | Country Template AU       | Australia                 | AU    | AUD   | E     | INT   | 10    | V6    |       |       |            |              |       |       |       |       |       | 00000000 | 2     |       |       | X     | X     |       | X     |       | X     |       | X     |       |       |       |       |       |       |       |       |       |       | AU01  |            | 0001  | 0001  |       |       |       |       |       |       |       |       | P0    | S0    |       |       |       |          |          |       |         |       |       | 0        |         |      |          |       |        |        |        |        |        |       |          |             |               |       |
| 000   | BE01  | Country Template BE       | Belgium                   | BE    | EUR   | E     | CABE  | 10    | K4    | 2     |       |            | BE000009797  |       |       |       |       |       | 00000000 | 2     |       |       | X     | X     | 0001  | X     |       | X     | X     |       |       | X     |       |       |       |       |       |       |       |       |       |            | 0001  | 0001  |       |       |       |       |       |       |       |       | V0    | A0    |       |       |       |          |          |       |         |       |       | 0        |         |      |          |       |        |        |        |        |        |       |          |             |               |       |
| 000   | BR01  | Country Template BR       | Brazil                    | BR    | BRL   | P     | INT   | 10    | K4    |       |       |            |              |       |       |       |       |       | 00000000 | 2     |       |       | X     |       |       |       |       |       | X     |       |       |       |       |       |       |       |       |       |       |       |       |            | 0001  | 0001  |       |       |       |       |       |       |       |       | I0    | A0    |       |       |       | X        |          |       |         |       |       | 0        |         |      |          |       |        |        |        |        |        |       |          |             |               |       |
| 000   | CA01  | Country Template CA       | Canada                    | CA    | CAD   | E     | CANA  | 10    | K4    | 2     |       |            |              |       |       |       |       |       | 00000000 | 2     |       |       | X     |       |       |       |       | X     |       |       |       |       |       | X     |       |       |       |       |       |       |       |            | 0001  | 0001  |       |       |       |       |       |       |       |       | V0    | A0    |       |       |       |          |          |       |         |       |       | 0        |         | 2    |          |       |        |        |        |        |        |       |          |             |               |       |
| 000   | CH01  | Country Template CH       | Switzerland               | CH    | CHF   | D     | CACH  | 10    | K4    | 2     |       |            |              |       |       |       |       |       | 00000000 | 1     |       |       | X     |       |       |       |       | X     |       |       |       |       |       | X     |       |       |       |       |       |       |       |            | 0001  | 0001  |       |       |       |       |       |       |       |       | V0    | A0    |       |       |       |          |          |       |         |       |       | 0        |         |      |          |       |        |        |        |        |        |       |          |             |               |       |
| 000   | CL01  | Country Template CL       | Chile                     | CL    | CLP   | S     | INT   | 10    | K4    |       |       | 0000026592 |              |       |       |       |       |       | 00000000 | 2     |       |       | X     |       |       |       |       |       | X     |       |       |       |       |       |       |       |       |       |       |       |       |            | 0001  | 0001  |       |       |       |       |       |       |       |       | C0    | D0    |       |       |       | X        |          |       |         |       |       | 0        |         |      |          |       |        |        |        |        |        |       |          |             |               |       |
| 000   | CN01  | Country Template CN       | China                     | CN    | CNY   | 1     | CACN  | 10    | K1    | 2     |       |            |              |       |       |       |       |       | 00000000 | 2     |       |       | X     |       | 0001  |       |       |       | X     |       |       |       |       |       |       |       |       |       |       |       |       |            | 0001  | 0001  |       |       |       |       |       |       |       |       | J0    | X0    |       | X     |       |          |          |       |         |       |       | 0        |         |      |          |       |        |        |        |        |        |       |          |             |               |       |
| 000   | CO01  | Country Template CO       | Colombia                  | CO    | COP   | S     | CACO  | 10    | K4    | 2     |       |            |              |       |       |       |       |       | 00000000 | 2     |       |       |       |       |       |       |       | X     |       |       |       |       |       |       |       |       |       |       |       |       |       |            | 0001  | 0001  |       |       |       |       |       |       |       |       | V0    | A0    |       |       |       | X        |          | CO01  |         |       |       | 0        |         |      |          |       |        |        |        |        |        |       |          |             |               |       |
| 000   | COPY  | Copy from CC.0001         | (Only G/L accounts B-seg) | DE    | EUR   | D     | INT   | 10    | K4    | 2     |       |            |              |       |       |       |       |       | 00000000 |       |       |       | X     |       |       |       |       | X     |       |       |       |       |       |       |       |       |       |       |       |       | 0001  |            | 0001  | 0001  |       |       |       |       |       |       |       |       | V0    | A0    |       |       |       |          |          |       |         |       |       | 0        |         |      |          |       |        |        |        |        |        |       |          |             |               |       |
| 000   | CZ01  | Country Template CZ       | Czech Republic            | CZ    | CZK   | C     | CACZ  | 05    | K4    | 2     |       |            | CZ1234567890 |       |       |       |       |       | 00000000 |       |       |       |       |       |       |       |       |       |       |       |       |       |       |       |       |       |       |       |       |       |       |            | 0001  | 0001  |       |       |       |       |       |       |       |       | I0    | E0    |       | X     |       |          |          |       |         |       |       | 0        |         |      |          |       |        |        |        |        |        |       |          |             |               |       |
| 000   | DE01  | Country Template DE       | Germany                   | DE    | EUR   | D     | GKR   | 10    | K4    | 2     |       |            |              |       |       |       |       |       | 00000000 |       |       |       | X     |       |       |       |       | X     |       |       |       |       |       | X     |       |       |       |       |       |       | 0001  |            | 0001  | 0001  |       |       |       |       |       |       |       |       | V0    | A0    |       |       |       |          |          |       |         |       |       | 0        |         |      |          |       |        |        |        |        |        |       |          |             |               |       |
| 000   | DE02  | Country Template DE       | Germany                   | DE    | EUR   | D     | IKR   | 10    | K4    | 2     |       |            |              |       |       |       |       |       | 00000000 |       |       |       | X     |       |       |       |       | X     |       |       |       |       |       | X     |       |       |       |       |       |       | 0001  |            | 0001  | 0001  |       |       |       |       |       |       |       |       | V0    | A0    |       |       |       |          |          |       |         |       |       | 0        |         |      |          |       |        |        |        |        |        |       |          |             |               |       |
| 000   | DK01  | Country Template DK       | Denmark                   | DK    | DKK   | K     | INT   | 10    | K4    |       |       |            |              |       |       |       |       |       | 00000000 |       |       |       | X     | X     |       |       |       | X     | X     | X     |       |       |       |       |       |       |       |       |       |       |       |            | 0001  | 0001  |       |       |       |       |       |       |       |       | K0    | S0    |       |       |       |          |          |       |         |       |       | 0        |         |      |          |       |        |        |        |        |        |       |          |             |               |       |
| 000   | ES01  | Country Template ES       | Spain                     | ES    | EUR   | S     | CAES  | 10    | K4    | 2     |       |            | ESA58379629  |       |       |       |       |       | 00000000 | 2     |       |       | X     |       | 0001  |       |       | X     | X     |       |       |       |       |       |       |       |       |       |       |       |       |            | 0001  | 0001  |       |       |       |       |       |       |       |       | S0    | R0    |       |       |       | X        |          |       |         |       |       | 0        |

</details>

We had the field FMHRDATE on the table, and it is mostly populated with all zeros.
Our target system have to convert the DATS field of the ABAP that support alphanumeric characters to an ANSI SQL Date.

So we came with two regular expressión:
1. The __"all zeros"__ or __"spaces"__ in the field is invalid and needed to be replace by a constant like '19000101'.
  We represent this with two regular expressions:
  1. `^0*$` this represent that all the values are '0'
  2. `^.*\s+$` this represent a string with one or more spaces
2. The other rule that we can use is the only valid values that in this field your be something like YYYYMMDD format.
  Here we come with the next expression that only take dates in format YYYYMMDD after the year 1899 `^(19\d\d|20[0-2]\d)(0[1-9]|1[012])(0[1-9]|[12]\d|3[01])$`
> **Note:**
> Sometimes the "correct value" it's more wide to cover so that is why we also include a not valid value.

We could pass these null treatment with the flag `--null_tratment` and the string in json format or the path to a json file that contain that rule as the [example](/samples/example_null_treatment_ABAP_date.json):
```json
[
  {
    "field": "FMHRDATE",
    "null_const": "19000101",
    "nulls": ["^0*$","^.*\\s+$"],
    "not_nulls": ["^(19\\d\\d|20[0-2]\\d)(0[1-9]|1[012])(0[1-9]|[12]\\d|3[01])$"]
  }
]
```

Also the data will be splitted by the fields MANDT and LAND1 in several files.

> **Warning**
> Due that the data will be download in small files we won't limit the size of each file.
> It's better to download the data in batches

The script will be called as follow:
```bash
python HANA_downloader.py "configure" --config_dir "~/test/configuration" \
--table "T001" --table_schema "ECC" --group MANDT LAND1 \
--null_treatment "/path_to_json/null.json" \
--download_dir "~/test/data" --download_mode local
```

If we make an `ls` on the directory set with `--config_dir` we will se something like these:
```
config_ECC_T001_MANDT-000_LAND1-.json config_ECC_T001_MANDT-000_LAND1-HU.json config_ECC_T001_MANDT-000_LAND1-QA.json config_ECC_T001_MANDT-001_LAND1-CL.json config_ECC_T001_MANDT-001_LAND1-MY.json config_ECC_T001_MANDT-400_LAND1-AR.json
config_ECC_T001_MANDT-000_LAND1-AR.json config_ECC_T001_MANDT-000_LAND1-ID.json config_ECC_T001_MANDT-000_LAND1-RU.json config_ECC_T001_MANDT-001_LAND1-CN.json config_ECC_T001_MANDT-001_LAND1-NL.json config_ECC_T001_MANDT-400_LAND1-AT.json
config_ECC_T001_MANDT-000_LAND1-AT.json config_ECC_T001_MANDT-000_LAND1-IE.json config_ECC_T001_MANDT-000_LAND1-SE.json config_ECC_T001_MANDT-001_LAND1-CO.json config_ECC_T001_MANDT-001_LAND1-NO.json config_ECC_T001_MANDT-400_LAND1-BR.json
config_ECC_T001_MANDT-000_LAND1-AU.json config_ECC_T001_MANDT-000_LAND1-IN.json config_ECC_T001_MANDT-000_LAND1-SG.json config_ECC_T001_MANDT-001_LAND1-CZ.json config_ECC_T001_MANDT-001_LAND1-NZ.json config_ECC_T001_MANDT-400_LAND1-CL.json
config_ECC_T001_MANDT-000_LAND1-BE.json config_ECC_T001_MANDT-000_LAND1-IT.json config_ECC_T001_MANDT-000_LAND1-SK.json config_ECC_T001_MANDT-001_LAND1-DE.json config_ECC_T001_MANDT-001_LAND1-PE.json config_ECC_T001_MANDT-400_LAND1-CO.json
config_ECC_T001_MANDT-000_LAND1-BR.json config_ECC_T001_MANDT-000_LAND1-JP.json config_ECC_T001_MANDT-000_LAND1-TH.json config_ECC_T001_MANDT-001_LAND1-DK.json config_ECC_T001_MANDT-001_LAND1-PH.json config_ECC_T001_MANDT-400_LAND1-DE.json
config_ECC_T001_MANDT-000_LAND1-CA.json config_ECC_T001_MANDT-000_LAND1-KR.json config_ECC_T001_MANDT-000_LAND1-TR.json config_ECC_T001_MANDT-001_LAND1-ES.json config_ECC_T001_MANDT-001_LAND1-PL.json config_ECC_T001_MANDT-400_LAND1-EC.json
config_ECC_T001_MANDT-000_LAND1-CH.json config_ECC_T001_MANDT-000_LAND1-KW.json config_ECC_T001_MANDT-000_LAND1-TW.json config_ECC_T001_MANDT-001_LAND1-FI.json config_ECC_T001_MANDT-001_LAND1-PT.json config_ECC_T001_MANDT-400_LAND1-FI.json
config_ECC_T001_MANDT-000_LAND1-CL.json config_ECC_T001_MANDT-000_LAND1-KZ.json config_ECC_T001_MANDT-000_LAND1-UA.json config_ECC_T001_MANDT-001_LAND1-FR.json config_ECC_T001_MANDT-001_LAND1-RU.json config_ECC_T001_MANDT-400_LAND1-GB.json
config_ECC_T001_MANDT-000_LAND1-CN.json config_ECC_T001_MANDT-000_LAND1-LU.json config_ECC_T001_MANDT-000_LAND1-US.json config_ECC_T001_MANDT-001_LAND1-GB.json config_ECC_T001_MANDT-001_LAND1-SE.json config_ECC_T001_MANDT-400_LAND1-KY.json
config_ECC_T001_MANDT-000_LAND1-CO.json config_ECC_T001_MANDT-000_LAND1-MX.json config_ECC_T001_MANDT-000_LAND1-VE.json config_ECC_T001_MANDT-001_LAND1-HK.json config_ECC_T001_MANDT-001_LAND1-SG.json config_ECC_T001_MANDT-400_LAND1-MX.json
config_ECC_T001_MANDT-000_LAND1-CZ.json config_ECC_T001_MANDT-000_LAND1-MY.json config_ECC_T001_MANDT-000_LAND1-ZA.json config_ECC_T001_MANDT-001_LAND1-HU.json config_ECC_T001_MANDT-001_LAND1-SK.json config_ECC_T001_MANDT-400_LAND1-PE.json
config_ECC_T001_MANDT-000_LAND1-DE.json config_ECC_T001_MANDT-000_LAND1-NL.json config_ECC_T001_MANDT-001_LAND1-AR.json config_ECC_T001_MANDT-001_LAND1-IE.json config_ECC_T001_MANDT-001_LAND1-TH.json config_ECC_T001_MANDT-400_LAND1-US.json
config_ECC_T001_MANDT-000_LAND1-DK.json config_ECC_T001_MANDT-000_LAND1-NO.json config_ECC_T001_MANDT-001_LAND1-AT.json config_ECC_T001_MANDT-001_LAND1-IT.json config_ECC_T001_MANDT-001_LAND1-TR.json config_ECC_T001_MANDT-400_LAND1-UY.json
config_ECC_T001_MANDT-000_LAND1-ES.json config_ECC_T001_MANDT-000_LAND1-NZ.json config_ECC_T001_MANDT-001_LAND1-AU.json config_ECC_T001_MANDT-001_LAND1-JP.json config_ECC_T001_MANDT-001_LAND1-TW.json
config_ECC_T001_MANDT-000_LAND1-FI.json config_ECC_T001_MANDT-000_LAND1-PE.json config_ECC_T001_MANDT-001_LAND1-BE.json config_ECC_T001_MANDT-001_LAND1-KR.json config_ECC_T001_MANDT-001_LAND1-UA.json
config_ECC_T001_MANDT-000_LAND1-FR.json config_ECC_T001_MANDT-000_LAND1-PH.json config_ECC_T001_MANDT-001_LAND1-BR.json config_ECC_T001_MANDT-001_LAND1-KZ.json config_ECC_T001_MANDT-001_LAND1-US.json
config_ECC_T001_MANDT-000_LAND1-GB.json config_ECC_T001_MANDT-000_LAND1-PL.json config_ECC_T001_MANDT-001_LAND1-CA.json config_ECC_T001_MANDT-001_LAND1-LU.json config_ECC_T001_MANDT-001_LAND1-VE.json
config_ECC_T001_MANDT-000_LAND1-HK.json config_ECC_T001_MANDT-000_LAND1-PT.json config_ECC_T001_MANDT-001_LAND1-CH.json config_ECC_T001_MANDT-001_LAND1-MX.json config_ECC_T001_MANDT-001_LAND1-ZA.json
```
All the files created with the configuration to download the data and splitted by the fields MANDT and LAND1.

<details>

<summary>Sample of file config\_ECC\_T001\_MANDT-000\_LAND1-DE.json</summary>

```json
[
    {
        "table_schema": "ECC",
        "table": "T001",
        "fields": {
            "MANDT": {
                "key": "X",
                "type": "NVARCHAR",
                "length": 3,
                "scale": null
            },
            "BUKRS": {
                "key": "X",
                "type": "NVARCHAR",
                "length": 4,
                "scale": null
            },
            "BUTXT": {
                "key": "",
                "type": "NVARCHAR",
                "length": 25,
                "scale": null
            },
            "ORT01": {
                "key": "",
                "type": "NVARCHAR",
                "length": 25,
                "scale": null
            },
            "LAND1": {
                "key": "",
                "type": "NVARCHAR",
                "length": 3,
                "scale": null
            },
            "WAERS": {
                "key": "",
                "type": "NVARCHAR",
                "length": 5,
                "scale": null
            },
            "SPRAS": {
                "key": "",
                "type": "NVARCHAR",
                "length": 1,
                "scale": null
            },
            "KTOPL": {
                "key": "",
                "type": "NVARCHAR",
                "length": 4,
                "scale": null
            },
            "WAABW": {
                "key": "",
                "type": "NVARCHAR",
                "length": 2,
                "scale": null
            },
            "PERIV": {
                "key": "",
                "type": "NVARCHAR",
                "length": 2,
                "scale": null
            },
            "KOKFI": {
                "key": "",
                "type": "NVARCHAR",
                "length": 1,
                "scale": null
            },
            "RCOMP": {
                "key": "",
                "type": "NVARCHAR",
                "length": 6,
                "scale": null
            },
            "ADRNR": {
                "key": "",
                "type": "NVARCHAR",
                "length": 10,
                "scale": null
            },
            "STCEG": {
                "key": "",
                "type": "NVARCHAR",
                "length": 20,
                "scale": null
            },
            "FIKRS": {
                "key": "",
                "type": "NVARCHAR",
                "length": 4,
                "scale": null
            },
            "XFMCO": {
                "key": "",
                "type": "NVARCHAR",
                "length": 1,
                "scale": null
            },
            "XFMCB": {
                "key": "",
                "type": "NVARCHAR",
                "length": 1,
                "scale": null
            },
            "XFMCA": {
                "key": "",
                "type": "NVARCHAR",
                "length": 1,
                "scale": null
            },
            "TXJCD": {
                "key": "",
                "type": "NVARCHAR",
                "length": 15,
                "scale": null
            },
            "FMHRDATE": {
                "key": "",
                "type": "NVARCHAR",
                "length": 8,
                "scale": null,
                "null_const": "19000101",
                "nulls": [
                    "^0*$",
                    "^.*\\s+$"
                ],
                "not_nulls": [
                    "^(19\\d\\d|20[0-2]\\d)(0[1-9]|1[012])(0[1-9]|[12]\\d|3[01])$"
                ]
            },
            "BUVAR": {
                "key": "",
                "type": "NVARCHAR",
                "length": 1,
                "scale": null
            },
            "FDBUK": {
                "key": "",
                "type": "NVARCHAR",
                "length": 4,
                "scale": null
            },
            "XFDIS": {
                "key": "",
                "type": "NVARCHAR",
                "length": 1,
                "scale": null
            },
            "XVALV": {
                "key": "",
                "type": "NVARCHAR",
                "length": 1,
                "scale": null
            },
            "XSKFN": {
                "key": "",
                "type": "NVARCHAR",
                "length": 1,
                "scale": null
            },
            "KKBER": {
                "key": "",
                "type": "NVARCHAR",
                "length": 4,
                "scale": null
            },
            "XMWSN": {
                "key": "",
                "type": "NVARCHAR",
                "length": 1,
                "scale": null
            },
            "MREGL": {
                "key": "",
                "type": "NVARCHAR",
                "length": 4,
                "scale": null
            },
            "XGSBE": {
                "key": "",
                "type": "NVARCHAR",
                "length": 1,
                "scale": null
            },
            "XGJRV": {
                "key": "",
                "type": "NVARCHAR",
                "length": 1,
                "scale": null
            },
            "XKDFT": {
                "key": "",
                "type": "NVARCHAR",
                "length": 1,
                "scale": null
            },
            "XPROD": {
                "key": "",
                "type": "NVARCHAR",
                "length": 1,
                "scale": null
            },
            "XEINK": {
                "key": "",
                "type": "NVARCHAR",
                "length": 1,
                "scale": null
            },
            "XJVAA": {
                "key": "",
                "type": "NVARCHAR",
                "length": 1,
                "scale": null
            },
            "XVVWA": {
                "key": "",
                "type": "NVARCHAR",
                "length": 1,
                "scale": null
            },
            "XSLTA": {
                "key": "",
                "type": "NVARCHAR",
                "length": 1,
                "scale": null
            },
            "XFDMM": {
                "key": "",
                "type": "NVARCHAR",
                "length": 1,
                "scale": null
            },
            "XFDSD": {
                "key": "",
                "type": "NVARCHAR",
                "length": 1,
                "scale": null
            },
            "XEXTB": {
                "key": "",
                "type": "NVARCHAR",
                "length": 1,
                "scale": null
            },
            "EBUKR": {
                "key": "",
                "type": "NVARCHAR",
                "length": 4,
                "scale": null
            },
            "KTOP2": {
                "key": "",
                "type": "NVARCHAR",
                "length": 4,
                "scale": null
            },
            "UMKRS": {
                "key": "",
                "type": "NVARCHAR",
                "length": 4,
                "scale": null
            },
            "BUKRS_GLOB": {
                "key": "",
                "type": "NVARCHAR",
                "length": 6,
                "scale": null
            },
            "FSTVA": {
                "key": "",
                "type": "NVARCHAR",
                "length": 4,
                "scale": null
            },
            "OPVAR": {
                "key": "",
                "type": "NVARCHAR",
                "length": 4,
                "scale": null
            },
            "XCOVR": {
                "key": "",
                "type": "NVARCHAR",
                "length": 1,
                "scale": null
            },
            "TXKRS": {
                "key": "",
                "type": "NVARCHAR",
                "length": 1,
                "scale": null
            },
            "WFVAR": {
                "key": "",
                "type": "NVARCHAR",
                "length": 4,
                "scale": null
            },
            "XBBBF": {
                "key": "",
                "type": "NVARCHAR",
                "length": 1,
                "scale": null
            },
            "XBBBE": {
                "key": "",
                "type": "NVARCHAR",
                "length": 1,
                "scale": null
            },
            "XBBBA": {
                "key": "",
                "type": "NVARCHAR",
                "length": 1,
                "scale": null
            },
            "XBBKO": {
                "key": "",
                "type": "NVARCHAR",
                "length": 1,
                "scale": null
            },
            "XSTDT": {
                "key": "",
                "type": "NVARCHAR",
                "length": 1,
                "scale": null
            },
            "MWSKV": {
                "key": "",
                "type": "NVARCHAR",
                "length": 2,
                "scale": null
            },
            "MWSKA": {
                "key": "",
                "type": "NVARCHAR",
                "length": 2,
                "scale": null
            },
            "IMPDA": {
                "key": "",
                "type": "NVARCHAR",
                "length": 1,
                "scale": null
            },
            "XNEGP": {
                "key": "",
                "type": "NVARCHAR",
                "length": 1,
                "scale": null
            },
            "XKKBI": {
                "key": "",
                "type": "NVARCHAR",
                "length": 1,
                "scale": null
            },
            "WT_NEWWT": {
                "key": "",
                "type": "NVARCHAR",
                "length": 1,
                "scale": null
            },
            "PP_PDATE": {
                "key": "",
                "type": "NVARCHAR",
                "length": 1,
                "scale": null
            },
            "INFMT": {
                "key": "",
                "type": "NVARCHAR",
                "length": 4,
                "scale": null
            },
            "FSTVARE": {
                "key": "",
                "type": "NVARCHAR",
                "length": 4,
                "scale": null
            },
            "KOPIM": {
                "key": "",
                "type": "NVARCHAR",
                "length": 1,
                "scale": null
            },
            "DKWEG": {
                "key": "",
                "type": "NVARCHAR",
                "length": 1,
                "scale": null
            },
            "OFFSACCT": {
                "key": "",
                "type": "NVARCHAR",
                "length": 1,
                "scale": null
            },
            "BAPOVAR": {
                "key": "",
                "type": "NVARCHAR",
                "length": 2,
                "scale": null
            },
            "XCOS": {
                "key": "",
                "type": "NVARCHAR",
                "length": 1,
                "scale": null
            },
            "XCESSION": {
                "key": "",
                "type": "NVARCHAR",
                "length": 1,
                "scale": null
            },
            "XSPLT": {
                "key": "",
                "type": "NVARCHAR",
                "length": 1,
                "scale": null
            },
            "SURCCM": {
                "key": "",
                "type": "NVARCHAR",
                "length": 1,
                "scale": null
            },
            "DTPROV": {
                "key": "",
                "type": "NVARCHAR",
                "length": 2,
                "scale": null
            },
            "DTAMTC": {
                "key": "",
                "type": "NVARCHAR",
                "length": 2,
                "scale": null
            },
            "DTTAXC": {
                "key": "",
                "type": "NVARCHAR",
                "length": 2,
                "scale": null
            },
            "DTTDSP": {
                "key": "",
                "type": "NVARCHAR",
                "length": 2,
                "scale": null
            },
            "DTAXR": {
                "key": "",
                "type": "NVARCHAR",
                "length": 4,
                "scale": null
            },
            "XVATDATE": {
                "key": "",
                "type": "NVARCHAR",
                "length": 1,
                "scale": null
            },
            "PST_PER_VAR": {
                "key": "",
                "type": "NVARCHAR",
                "length": 1,
                "scale": null
            },
            "FM_DERIVE_ACC": {
                "key": "",
                "type": "NVARCHAR",
                "length": 1,
                "scale": null
            },
            "XBBSC": {
                "key": "",
                "type": "NVARCHAR",
                "length": 1,
                "scale": null
            }
        },
        "download_dir": "/test/data",
        "download_mode": "local",
        "grouping": [
            {
                "field": "MANDT",
                "value": "000"
            },
            {
                "field": "LAND1",
                "value": "DE"
            }
        ]
    }
]

```

</details>

Once all the files with the configuration were created, the script will be executed using the path to those files to take them as the recipe to download the data.
To use them we execute the script as follow:
```bash
HANA_downloader.py "download" --table "T001" --table_schema "ECC" \
--group MANDT LAND1  --download_dir "~/test/data" --download_mode local \
--null_treatment "~/test/samples/example_null_treatment_ABAP_date.json"
```
Or just take the directory where all the files with the recipe are it's enough:
```bash
HANA_downloader.py "download" --config_dir "~/test/configuration"
```

And for the json files we provide, the parquet files compressed with snappy will be created.

<details>

<summary>Example of a `ls -lh` on the directory `~/test/data`</summary>

```bash
total 9600
-rw-r--r--  1 osuser  staff    41K Mar 14 15:18 data_ECC_T001_MANDT-000_LAND1-AR_20230314151823527401.parquet.snappy
-rw-r--r--  1 osuser  staff    41K Mar 14 15:17 data_ECC_T001_MANDT-000_LAND1-AT_20230314151744013946.parquet.snappy
-rw-r--r--  1 osuser  staff    41K Mar 14 15:16 data_ECC_T001_MANDT-000_LAND1-AU_20230314151649190184.parquet.snappy
-rw-r--r--  1 osuser  staff    42K Mar 14 15:19 data_ECC_T001_MANDT-000_LAND1-BE_20230314151947407059.parquet.snappy
-rw-r--r--  1 osuser  staff    41K Mar 14 15:17 data_ECC_T001_MANDT-000_LAND1-BR_20230314151723145060.parquet.snappy
-rw-r--r--  1 osuser  staff    41K Mar 14 15:17 data_ECC_T001_MANDT-000_LAND1-CA_20230314151728850841.parquet.snappy
-rw-r--r--  1 osuser  staff    41K Mar 14 15:18 data_ECC_T001_MANDT-000_LAND1-CH_20230314151852130564.parquet.snappy
-rw-r--r--  1 osuser  staff    41K Mar 14 15:16 data_ECC_T001_MANDT-000_LAND1-CL_20230314151647317280.parquet.snappy
-rw-r--r--  1 osuser  staff    41K Mar 14 15:19 data_ECC_T001_MANDT-000_LAND1-CN_20230314151926420396.parquet.snappy
-rw-r--r--  1 osuser  staff    41K Mar 14 15:19 data_ECC_T001_MANDT-000_LAND1-CO_20230314151958815598.parquet.snappy
-rw-r--r--  1 osuser  staff    41K Mar 14 15:20 data_ECC_T001_MANDT-000_LAND1-CZ_20230314152008261073.parquet.snappy
-rw-r--r--  1 osuser  staff    42K Mar 14 15:19 data_ECC_T001_MANDT-000_LAND1-DE_20230314151918594833.parquet.snappy
-rw-r--r--  1 osuser  staff    41K Mar 14 15:17 data_ECC_T001_MANDT-000_LAND1-DK_20230314151715547941.parquet.snappy
-rw-r--r--  1 osuser  staff    41K Mar 14 15:18 data_ECC_T001_MANDT-000_LAND1-ES_20230314151819625139.parquet.snappy
-rw-r--r--  1 osuser  staff    41K Mar 14 15:19 data_ECC_T001_MANDT-000_LAND1-FI_20230314151928299094.parquet.snappy
-rw-r--r--  1 osuser  staff    41K Mar 14 15:17 data_ECC_T001_MANDT-000_LAND1-FR_20230314151713641675.parquet.snappy
-rw-r--r--  1 osuser  staff    42K Mar 14 15:19 data_ECC_T001_MANDT-000_LAND1-GB_20230314151951217898.parquet.snappy
-rw-r--r--  1 osuser  staff    41K Mar 14 15:19 data_ECC_T001_MANDT-000_LAND1-HK_20230314151939658758.parquet.snappy
-rw-r--r--  1 osuser  staff    41K Mar 14 15:18 data_ECC_T001_MANDT-000_LAND1-HU_20230314151848385098.parquet.snappy
-rw-r--r--  1 osuser  staff    41K Mar 14 15:17 data_ECC_T001_MANDT-000_LAND1-ID_20230314151758990987.parquet.snappy
-rw-r--r--  1 osuser  staff    41K Mar 14 15:18 data_ECC_T001_MANDT-000_LAND1-IE_20230314151817763211.parquet.snappy
-rw-r--r--  1 osuser  staff    42K Mar 14 15:17 data_ECC_T001_MANDT-000_LAND1-IN_20230314151707935374.parquet.snappy
-rw-r--r--  1 osuser  staff    41K Mar 14 15:20 data_ECC_T001_MANDT-000_LAND1-IT_20230314152006384080.parquet.snappy
-rw-r--r--  1 osuser  staff    41K Mar 14 15:17 data_ECC_T001_MANDT-000_LAND1-JP_20230314151732620076.parquet.snappy
-rw-r--r--  1 osuser  staff    41K Mar 14 15:19 data_ECC_T001_MANDT-000_LAND1-KR_20230314151905324158.parquet.snappy
-rw-r--r--  1 osuser  staff    41K Mar 14 15:17 data_ECC_T001_MANDT-000_LAND1-KW_20230314151709811209.parquet.snappy
-rw-r--r--  1 osuser  staff    41K Mar 14 15:17 data_ECC_T001_MANDT-000_LAND1-KZ_20230314151740277626.parquet.snappy
-rw-r--r--  1 osuser  staff    42K Mar 14 15:18 data_ECC_T001_MANDT-000_LAND1-LU_20230314151853994842.parquet.snappy
-rw-r--r--  1 osuser  staff    41K Mar 14 15:19 data_ECC_T001_MANDT-000_LAND1-MX_20230314151930181437.parquet.snappy
-rw-r--r--  1 osuser  staff    41K Mar 14 15:19 data_ECC_T001_MANDT-000_LAND1-MY_20230314151956953454.parquet.snappy
-rw-r--r--  1 osuser  staff    42K Mar 14 15:18 data_ECC_T001_MANDT-000_LAND1-NL_20230314151855911529.parquet.snappy
-rw-r--r--  1 osuser  staff    41K Mar 14 15:18 data_ECC_T001_MANDT-000_LAND1-NO_20230314151802718822.parquet.snappy
-rw-r--r--  1 osuser  staff    41K Mar 14 15:17 data_ECC_T001_MANDT-000_LAND1-NZ_20230314151757143299.parquet.snappy
-rw-r--r--  1 osuser  staff    41K Mar 14 15:18 data_ECC_T001_MANDT-000_LAND1-PE_20230314151831095173.parquet.snappy
-rw-r--r--  1 osuser  staff    41K Mar 14 15:18 data_ECC_T001_MANDT-000_LAND1-PH_20230314151804614748.parquet.snappy
-rw-r--r--  1 osuser  staff    41K Mar 14 15:19 data_ECC_T001_MANDT-000_LAND1-PL_20230314151920482477.parquet.snappy
-rw-r--r--  1 osuser  staff    41K Mar 14 15:19 data_ECC_T001_MANDT-000_LAND1-PT_20230314151943503733.parquet.snappy
-rw-r--r--  1 osuser  staff    41K Mar 14 15:19 data_ECC_T001_MANDT-000_LAND1-QA_20230314151909095691.parquet.snappy
-rw-r--r--  1 osuser  staff    41K Mar 14 15:19 data_ECC_T001_MANDT-000_LAND1-RU_20230314151922384548.parquet.snappy
-rw-r--r--  1 osuser  staff    41K Mar 14 15:17 data_ECC_T001_MANDT-000_LAND1-SE_20230314151742139345.parquet.snappy
-rw-r--r--  1 osuser  staff    41K Mar 14 15:20 data_ECC_T001_MANDT-000_LAND1-SG_20230314152000700481.parquet.snappy
-rw-r--r--  1 osuser  staff    42K Mar 14 15:19 data_ECC_T001_MANDT-000_LAND1-SK_20230314151941596784.parquet.snappy
-rw-r--r--  1 osuser  staff    41K Mar 14 15:18 data_ECC_T001_MANDT-000_LAND1-TH_20230314151815914836.parquet.snappy
-rw-r--r--  1 osuser  staff    41K Mar 14 15:18 data_ECC_T001_MANDT-000_LAND1-TR_20230314151844239146.parquet.snappy
-rw-r--r--  1 osuser  staff    41K Mar 14 15:17 data_ECC_T001_MANDT-000_LAND1-TW_20230314151734561809.parquet.snappy
-rw-r--r--  1 osuser  staff    41K Mar 14 15:18 data_ECC_T001_MANDT-000_LAND1-UA_20230314151836636657.parquet.snappy
-rw-r--r--  1 osuser  staff    42K Mar 14 15:19 data_ECC_T001_MANDT-000_LAND1-US_20230314151949312666.parquet.snappy
-rw-r--r--  1 osuser  staff    41K Mar 14 15:17 data_ECC_T001_MANDT-000_LAND1-VE_20230314151755266313.parquet.snappy
-rw-r--r--  1 osuser  staff    41K Mar 14 15:17 data_ECC_T001_MANDT-000_LAND1-ZA_20230314151736454778.parquet.snappy
-rw-r--r--  1 osuser  staff    41K Mar 14 15:16 data_ECC_T001_MANDT-000_LAND1-_20230314151652939582.parquet.snappy
-rw-r--r--  1 osuser  staff    41K Mar 14 15:18 data_ECC_T001_MANDT-001_LAND1-AR_20230314151834788057.parquet.snappy
-rw-r--r--  1 osuser  staff    41K Mar 14 15:19 data_ECC_T001_MANDT-001_LAND1-AT_20230314151933983253.parquet.snappy
-rw-r--r--  1 osuser  staff    41K Mar 14 15:19 data_ECC_T001_MANDT-001_LAND1-AU_20230314151955007271.parquet.snappy
-rw-r--r--  1 osuser  staff    42K Mar 14 15:16 data_ECC_T001_MANDT-001_LAND1-BE_20230314151658599804.parquet.snappy
-rw-r--r--  1 osuser  staff    41K Mar 14 15:19 data_ECC_T001_MANDT-001_LAND1-BR_20230314151924286170.parquet.snappy
-rw-r--r--  1 osuser  staff    41K Mar 14 15:19 data_ECC_T001_MANDT-001_LAND1-CA_20230314151916728318.parquet.snappy
-rw-r--r--  1 osuser  staff    41K Mar 14 15:18 data_ECC_T001_MANDT-001_LAND1-CH_20230314151825384199.parquet.snappy
-rw-r--r--  1 osuser  staff    41K Mar 14 15:19 data_ECC_T001_MANDT-001_LAND1-CL_20230314151953110249.parquet.snappy
-rw-r--r--  1 osuser  staff    41K Mar 14 15:17 data_ECC_T001_MANDT-001_LAND1-CN_20230314151721245031.parquet.snappy
-rw-r--r--  1 osuser  staff    41K Mar 14 15:17 data_ECC_T001_MANDT-001_LAND1-CO_20230314151711758404.parquet.snappy
-rw-r--r--  1 osuser  staff    41K Mar 14 15:17 data_ECC_T001_MANDT-001_LAND1-CZ_20230314151704212044.parquet.snappy
-rw-r--r--  1 osuser  staff    42K Mar 14 15:17 data_ECC_T001_MANDT-001_LAND1-DE_20230314151730755963.parquet.snappy
-rw-r--r--  1 osuser  staff    41K Mar 14 15:20 data_ECC_T001_MANDT-001_LAND1-DK_20230314152004515644.parquet.snappy
-rw-r--r--  1 osuser  staff    41K Mar 14 15:18 data_ECC_T001_MANDT-001_LAND1-ES_20230314151838527838.parquet.snappy
-rw-r--r--  1 osuser  staff    41K Mar 14 15:17 data_ECC_T001_MANDT-001_LAND1-FI_20230314151719322409.parquet.snappy
-rw-r--r--  1 osuser  staff    41K Mar 14 15:20 data_ECC_T001_MANDT-001_LAND1-FR_20230314152002641355.parquet.snappy
-rw-r--r--  1 osuser  staff    42K Mar 14 15:16 data_ECC_T001_MANDT-001_LAND1-GB_20230314151654851018.parquet.snappy
-rw-r--r--  1 osuser  staff    41K Mar 14 15:17 data_ECC_T001_MANDT-001_LAND1-HK_20230314151702366448.parquet.snappy
-rw-r--r--  1 osuser  staff    41K Mar 14 15:18 data_ECC_T001_MANDT-001_LAND1-HU_20230314151832937112.parquet.snappy
-rw-r--r--  1 osuser  staff    41K Mar 14 15:18 data_ECC_T001_MANDT-001_LAND1-IE_20230314151840441892.parquet.snappy
-rw-r--r--  1 osuser  staff    41K Mar 14 15:17 data_ECC_T001_MANDT-001_LAND1-IT_20230314151706103747.parquet.snappy
-rw-r--r--  1 osuser  staff    41K Mar 14 15:19 data_ECC_T001_MANDT-001_LAND1-JP_20230314151912887034.parquet.snappy
-rw-r--r--  1 osuser  staff    41K Mar 14 15:18 data_ECC_T001_MANDT-001_LAND1-KR_20230314151806475784.parquet.snappy
-rw-r--r--  1 osuser  staff    41K Mar 14 15:19 data_ECC_T001_MANDT-001_LAND1-KZ_20230314151935858502.parquet.snappy
-rw-r--r--  1 osuser  staff    42K Mar 14 15:17 data_ECC_T001_MANDT-001_LAND1-LU_20230314151749647601.parquet.snappy
-rw-r--r--  1 osuser  staff    41K Mar 14 15:17 data_ECC_T001_MANDT-001_LAND1-MX_20230314151745897303.parquet.snappy
-rw-r--r--  1 osuser  staff    41K Mar 14 15:16 data_ECC_T001_MANDT-001_LAND1-MY_20230314151645435953.parquet.snappy
-rw-r--r--  1 osuser  staff    42K Mar 14 15:17 data_ECC_T001_MANDT-001_LAND1-NL_20230314151751491129.parquet.snappy
-rw-r--r--  1 osuser  staff    41K Mar 14 15:19 data_ECC_T001_MANDT-001_LAND1-NO_20230314151901540623.parquet.snappy
-rw-r--r--  1 osuser  staff    41K Mar 14 15:19 data_ECC_T001_MANDT-001_LAND1-NZ_20230314151907209532.parquet.snappy
-rw-r--r--  1 osuser  staff    41K Mar 14 15:18 data_ECC_T001_MANDT-001_LAND1-PE_20230314151850254214.parquet.snappy
-rw-r--r--  1 osuser  staff    41K Mar 14 15:19 data_ECC_T001_MANDT-001_LAND1-PH_20230314151903427263.parquet.snappy
-rw-r--r--  1 osuser  staff    41K Mar 14 15:17 data_ECC_T001_MANDT-001_LAND1-PL_20230314151725020951.parquet.snappy
-rw-r--r--  1 osuser  staff    41K Mar 14 15:16 data_ECC_T001_MANDT-001_LAND1-PT_20230314151656707407.parquet.snappy
-rw-r--r--  1 osuser  staff    41K Mar 14 15:17 data_ECC_T001_MANDT-001_LAND1-RU_20230314151726933209.parquet.snappy
-rw-r--r--  1 osuser  staff    41K Mar 14 15:19 data_ECC_T001_MANDT-001_LAND1-SE_20230314151932070272.parquet.snappy
-rw-r--r--  1 osuser  staff    41K Mar 14 15:17 data_ECC_T001_MANDT-001_LAND1-SG_20230314151717436641.parquet.snappy
-rw-r--r--  1 osuser  staff    41K Mar 14 15:17 data_ECC_T001_MANDT-001_LAND1-SK_20230314151700473389.parquet.snappy
-rw-r--r--  1 osuser  staff    41K Mar 14 15:18 data_ECC_T001_MANDT-001_LAND1-TH_20230314151842330200.parquet.snappy
-rw-r--r--  1 osuser  staff    41K Mar 14 15:18 data_ECC_T001_MANDT-001_LAND1-TR_20230314151814001981.parquet.snappy
-rw-r--r--  1 osuser  staff    41K Mar 14 15:19 data_ECC_T001_MANDT-001_LAND1-TW_20230314151914804512.parquet.snappy
-rw-r--r--  1 osuser  staff    41K Mar 14 15:18 data_ECC_T001_MANDT-001_LAND1-UA_20230314151821579991.parquet.snappy
-rw-r--r--  1 osuser  staff    41K Mar 14 15:16 data_ECC_T001_MANDT-001_LAND1-US_20230314151651102182.parquet.snappy
-rw-r--r--  1 osuser  staff    41K Mar 14 15:19 data_ECC_T001_MANDT-001_LAND1-VE_20230314151910998873.parquet.snappy
-rw-r--r--  1 osuser  staff    41K Mar 14 15:19 data_ECC_T001_MANDT-001_LAND1-ZA_20230314151937744284.parquet.snappy
-rw-r--r--  1 osuser  staff    42K Mar 14 15:20 data_ECC_T001_MANDT-400_LAND1-AR_20230314152010221363.parquet.snappy
-rw-r--r--  1 osuser  staff    41K Mar 14 15:18 data_ECC_T001_MANDT-400_LAND1-AT_20230314151857805712.parquet.snappy
-rw-r--r--  1 osuser  staff    42K Mar 14 15:18 data_ECC_T001_MANDT-400_LAND1-BR_20230314151859661679.parquet.snappy
-rw-r--r--  1 osuser  staff    44K Mar 14 15:18 data_ECC_T001_MANDT-400_LAND1-CL_20230314151846498241.parquet.snappy
-rw-r--r--  1 osuser  staff    42K Mar 14 15:18 data_ECC_T001_MANDT-400_LAND1-CO_20230314151812117976.parquet.snappy
-rw-r--r--  1 osuser  staff    41K Mar 14 15:17 data_ECC_T001_MANDT-400_LAND1-DE_20230314151753379813.parquet.snappy
-rw-r--r--  1 osuser  staff    42K Mar 14 15:18 data_ECC_T001_MANDT-400_LAND1-EC_20230314151800870272.parquet.snappy
-rw-r--r--  1 osuser  staff    42K Mar 14 15:18 data_ECC_T001_MANDT-400_LAND1-FI_20230314151810230675.parquet.snappy
-rw-r--r--  1 osuser  staff    42K Mar 14 15:18 data_ECC_T001_MANDT-400_LAND1-GB_20230314151829133363.parquet.snappy
-rw-r--r--  1 osuser  staff    42K Mar 14 15:18 data_ECC_T001_MANDT-400_LAND1-KY_20230314151808386428.parquet.snappy
-rw-r--r--  1 osuser  staff    42K Mar 14 15:17 data_ECC_T001_MANDT-400_LAND1-MX_20230314151747761663.parquet.snappy
-rw-r--r--  1 osuser  staff    42K Mar 14 15:19 data_ECC_T001_MANDT-400_LAND1-PE_20230314151945536565.parquet.snappy
-rw-r--r--  1 osuser  staff    42K Mar 14 15:18 data_ECC_T001_MANDT-400_LAND1-US_20230314151827281853.parquet.snappy
-rw-r--r--  1 osuser  staff    42K Mar 14 15:17 data_ECC_T001_MANDT-400_LAND1-UY_20230314151738380451.parquet.snappy
```

</details>

If we inspect the file with the `parq CLI` executing something like:
```bash
parq data_ECC_T001_MANDT-000_LAND1-DE_20230314151918594833.parquet.snappy
```
We can see an overview
```bash
# Metadata
<pyarrow._parquet.FileMetaData object at 0x10eb4de90>
 created_by: parquet-cpp-arrow version 11.0.0
 num_columns: 79
 num_rows: 13
 num_row_groups: 1
 format_version: 2.6
 serialized_size: 32900
```

<details>

<summary>Or check for the schema of the file:</summary>

```bash
parq data_ECC_T001_MANDT-000_LAND1-DE_20230314151918594833.parquet.snappy -s
```
And getting the list of columns:
```bash
 # Schema
 <pyarrow._parquet.ParquetSchema object at 0x12bc9f230>
required group field_id=-1 schema {
  optional binary field_id=-1 MANDT (String);
  optional binary field_id=-1 BUKRS (String);
  optional binary field_id=-1 BUTXT (String);
  optional binary field_id=-1 ORT01 (String);
  optional binary field_id=-1 LAND1 (String);
  optional binary field_id=-1 WAERS (String);
  optional binary field_id=-1 SPRAS (String);
  optional binary field_id=-1 KTOPL (String);
  optional binary field_id=-1 WAABW (String);
  optional binary field_id=-1 PERIV (String);
  optional binary field_id=-1 KOKFI (String);
  optional binary field_id=-1 RCOMP (String);
  optional binary field_id=-1 ADRNR (String);
  optional binary field_id=-1 STCEG (String);
  optional binary field_id=-1 FIKRS (String);
  optional binary field_id=-1 XFMCO (String);
  optional binary field_id=-1 XFMCB (String);
  optional binary field_id=-1 XFMCA (String);
  optional binary field_id=-1 TXJCD (String);
  optional binary field_id=-1 FMHRDATE (String);
  optional binary field_id=-1 BUVAR (String);
  optional binary field_id=-1 FDBUK (String);
  optional binary field_id=-1 XFDIS (String);
  optional binary field_id=-1 XVALV (String);
  optional binary field_id=-1 XSKFN (String);
  optional binary field_id=-1 KKBER (String);
  optional binary field_id=-1 XMWSN (String);
  optional binary field_id=-1 MREGL (String);
  optional binary field_id=-1 XGSBE (String);
  optional binary field_id=-1 XGJRV (String);
  optional binary field_id=-1 XKDFT (String);
  optional binary field_id=-1 XPROD (String);
  optional binary field_id=-1 XEINK (String);
  optional binary field_id=-1 XJVAA (String);
  optional binary field_id=-1 XVVWA (String);
  optional binary field_id=-1 XSLTA (String);
  optional binary field_id=-1 XFDMM (String);
  optional binary field_id=-1 XFDSD (String);
  optional binary field_id=-1 XEXTB (String);
  optional binary field_id=-1 EBUKR (String);
  optional binary field_id=-1 KTOP2 (String);
  optional binary field_id=-1 UMKRS (String);
  optional binary field_id=-1 BUKRS_GLOB (String);
  optional binary field_id=-1 FSTVA (String);
  optional binary field_id=-1 OPVAR (String);
  optional binary field_id=-1 XCOVR (String);
  optional binary field_id=-1 TXKRS (String);
  optional binary field_id=-1 WFVAR (String);
  optional binary field_id=-1 XBBBF (String);
  optional binary field_id=-1 XBBBE (String);
  optional binary field_id=-1 XBBBA (String);
  optional binary field_id=-1 XBBKO (String);
  optional binary field_id=-1 XSTDT (String);
  optional binary field_id=-1 MWSKV (String);
  optional binary field_id=-1 MWSKA (String);
  optional binary field_id=-1 IMPDA (String);
  optional binary field_id=-1 XNEGP (String);
  optional binary field_id=-1 XKKBI (String);
  optional binary field_id=-1 WT_NEWWT (String);
  optional binary field_id=-1 PP_PDATE (String);
  optional binary field_id=-1 INFMT (String);
  optional binary field_id=-1 FSTVARE (String);
  optional binary field_id=-1 KOPIM (String);
  optional binary field_id=-1 DKWEG (String);
  optional binary field_id=-1 OFFSACCT (String);
  optional binary field_id=-1 BAPOVAR (String);
  optional binary field_id=-1 XCOS (String);
  optional binary field_id=-1 XCESSION (String);
  optional binary field_id=-1 XSPLT (String);
  optional binary field_id=-1 SURCCM (String);
  optional binary field_id=-1 DTPROV (String);
  optional binary field_id=-1 DTAMTC (String);
  optional binary field_id=-1 DTTAXC (String);
  optional binary field_id=-1 DTTDSP (String);
  optional binary field_id=-1 DTAXR (String);
  optional binary field_id=-1 XVATDATE (String);
  optional binary field_id=-1 PST_PER_VAR (String);
  optional binary field_id=-1 FM_DERIVE_ACC (String);
  optional binary field_id=-1 XBBSC (String);
}
```
</details>


<details>

<summary>Or even part of the data with:</summary>

```bash
parq data_ECC_T001_MANDT-000_LAND1-DE_20230314151918594833.parquet.snappy -head
```
And getting the list of columns:
```bash
 0   000  0001                   SAP A.G.                   Walldorf    DE
 1   000  0MB1  IS-B Musterbank Deutschl.                   Walldorf    DE
 2   000  COPY          Copy from CC.0001  (Only G/L accounts B-seg)    DE
 3   000  DE01        Country Template DE                    Germany    DE
 4   000  DE02        Country Template DE                    Germany    DE
 5   000   ICS                   SAP A.G.                   Walldorf    DE
 6   000  MCA1   MCA Bank Backpack (bal.)                   Walldorf    DE
 7   000  MCA2   MCA Bank Backpack (doc.)                   Walldorf    DE
 8   000  MCA3     MCA Bank 4-pack (bal.)                   Walldorf    DE
 9   000  MCA4     MCA Bank 4-pack (doc.)                   Walldorf    DE

   WAERS SPRAS KTOPL WAABW PERIV KOKFI RCOMP       ADRNR STCEG FIKRS XFMCO  \
 0   EUR     D   INT    10    K4     2
 1   EUR     D  0MB1    00    K4     1
 2   EUR     D   INT    10    K4     2
 3   EUR     D   GKR    10    K4     2
 4   EUR     D   IKR    10    K4     2
 5   EUR     D   INT    10    K4              0000012763
 6   USD     D  BKMG    00    K4     2   MCA
 7   USD     D  BKMG    00    K4     2   MCA
 8   CHF     D  BKMG    00    K4     2   MCA
 9   CHF     D  BKMG    00    K4     2   MCA

   XFMCB XFMCA TXJCD  FMHRDATE BUVAR FDBUK XFDIS XVALV XSKFN KKBER XMWSN MREGL  \
 0                    19000101                                0001
 1                    19000101
 2                    19000101                       X
 3                    19000101                       X
 4                    19000101                       X
 5                    19000101     2                 X        0001
 6                    19000101
 7                    19000101
 8                    19000101
 9                    19000101

   XGSBE XGJRV XKDFT XPROD XEINK XJVAA XVVWA XSLTA XFDMM XFDSD XEXTB EBUKR  \
 0           X                             X
 1
 2     X
 3     X                                   X
 4     X                                   X
 5           X                 X           X
 6           X
 7           X
 8           X
 9           X

   KTOP2 UMKRS BUKRS_GLOB FSTVA OPVAR XCOVR TXKRS WFVAR XBBBF XBBBE XBBBA  \
 0        0001             0001  0001              0001
 1                         0001  0001
 2        0001             0001  0001
 3        0001             0001  0001
 4        0001             0001  0001
 5                         0001  0001              0001
 6                         0001   MCA
 7                         0001   MCA
 8                         0001   MCA
 9                         0001   MCA

   XBBKO XSTDT MWSKV MWSKA IMPDA XNEGP XKKBI WT_NEWWT PP_PDATE INFMT FSTVARE  \
 0                V0    A0     1           X                            FMRE
 1
 2                V0    A0
 3                V0    A0
 4                V0    A0
 5
 6                V0    A0
 7                V0    A0
 8                V0    A0
 9                V0    A0

   KOPIM DKWEG OFFSACCT BAPOVAR XCOS XCESSION XSPLT SURCCM DTPROV DTAMTC  \
 0                    0
 1                    0
 2                    0
 3                    0
 4                    0
 5                    0
 6                    0
 7                    0
 8                    0
 9                    0

   DTTAXC DTTDSP DTAXR XVATDATE PST_PER_VAR FM_DERIVE_ACC XBBSC
 0
 1
 2
 3
 4
 5
 6
 7
 8
 9
```
</details>

Here we can see that all the '00000000' on the FMHRDATE were replaced by '19000101'

# Another example to split create a config file with a limit of file size of each parquet.

We can check the overall footprint of the table, in these example the `LFB1` by executing directly the statement:
```sql
select * from sys.m_tables where table_name in('LFB1');
```
And will get something like this:
| SCHEMA_NAME | TABLE_NAME | RECORD_COUNT | TABLE_SIZE | IS_COLUMN_TABLE | TABLE_TYPE | IS_PARTITIONED | IS_REPLICATED |
|-------------|------------|--------------|------------|-----------------|------------|----------------|---------------|
| ECC         | LFB1       | 449619       | 15539092   | TRUE            | COLUMN     | FALSE          | FALSE         |

The size of the table on the DB it is __15,5 MB__

So to split the result in chunks of 5 MB we will execute
```bash
python HANA_downloader.py "configure" --config_dir "~/test/configuration/LFB1" \
--table "LFB1" --table_schema "ECC" \
--limit_mode MB --limit_num 5 \
--download_dir "~/test/data/LFB1" --download_mode local
```

We make an `ls` on the directory `~/test/configuration/LFB1` and we get only one file `config_ECC_LFB1.json`
<details>

<summary>Sample of the config\_ECC\_LFB1.json file:</summary>

```json
[
    {
        "table_schema": "ECC",
        "table": "LFB1",
        "rec_size": 151701,
        "fields": {
            "MANDT": {
                "key": "X",
                "type": "NVARCHAR",
                "length": 3,
                "scale": null
            },
            "LIFNR": {
                "key": "X",
                "type": "NVARCHAR",
                "length": 10,
                "scale": null
            },
            "BUKRS": {
                "key": "X",
                "type": "NVARCHAR",
                "length": 4,
                "scale": null
            },
            "PERNR": {
                "key": "",
                "type": "NVARCHAR",
                "length": 8,
                "scale": null
            },
            "ERDAT": {
                "key": "",
                "type": "NVARCHAR",
                "length": 8,
                "scale": null
            },
            "ERNAM": {
                "key": "",
                "type": "NVARCHAR",
                "length": 12,
                "scale": null
            },
            "SPERR": {
                "key": "",
                "type": "NVARCHAR",
                "length": 1,
                "scale": null
            },
            "LOEVM": {
                "key": "",
                "type": "NVARCHAR",
                "length": 1,
                "scale": null
            },
            "ZUAWA": {
                "key": "",
                "type": "NVARCHAR",
                "length": 3,
                "scale": null
            },
            "AKONT": {
                "key": "",
                "type": "NVARCHAR",
                "length": 10,
                "scale": null
            },
            "BEGRU": {
                "key": "",
                "type": "NVARCHAR",
                "length": 4,
                "scale": null
            },
            "VZSKZ": {
                "key": "",
                "type": "NVARCHAR",
                "length": 2,
                "scale": null
            },
            "ZWELS": {
                "key": "",
                "type": "NVARCHAR",
                "length": 10,
                "scale": null
            },
            "XVERR": {
                "key": "",
                "type": "NVARCHAR",
                "length": 1,
                "scale": null
            },
            "ZAHLS": {
                "key": "",
                "type": "NVARCHAR",
                "length": 1,
                "scale": null
            },
            "ZTERM": {
                "key": "",
                "type": "NVARCHAR",
                "length": 4,
                "scale": null
            },
            "EIKTO": {
                "key": "",
                "type": "NVARCHAR",
                "length": 12,
                "scale": null
            },
            "ZSABE": {
                "key": "",
                "type": "NVARCHAR",
                "length": 15,
                "scale": null
            },
            "KVERM": {
                "key": "",
                "type": "NVARCHAR",
                "length": 30,
                "scale": null
            },
            "FDGRV": {
                "key": "",
                "type": "NVARCHAR",
                "length": 10,
                "scale": null
            },
            "BUSAB": {
                "key": "",
                "type": "NVARCHAR",
                "length": 2,
                "scale": null
            },
            "LNRZE": {
                "key": "",
                "type": "NVARCHAR",
                "length": 10,
                "scale": null
            },
            "LNRZB": {
                "key": "",
                "type": "NVARCHAR",
                "length": 10,
                "scale": null
            },
            "ZINDT": {
                "key": "",
                "type": "NVARCHAR",
                "length": 8,
                "scale": null
            },
            "ZINRT": {
                "key": "",
                "type": "NVARCHAR",
                "length": 2,
                "scale": null
            },
            "DATLZ": {
                "key": "",
                "type": "NVARCHAR",
                "length": 8,
                "scale": null
            },
            "XDEZV": {
                "key": "",
                "type": "NVARCHAR",
                "length": 1,
                "scale": null
            },
            "WEBTR": {
                "key": "",
                "type": "DECIMAL",
                "length": 13,
                "scale": 2
            },
            "KULTG": {
                "key": "",
                "type": "DECIMAL",
                "length": 3,
                "scale": 0
            },
            "REPRF": {
                "key": "",
                "type": "NVARCHAR",
                "length": 1,
                "scale": null
            },
            "TOGRU": {
                "key": "",
                "type": "NVARCHAR",
                "length": 4,
                "scale": null
            },
            "HBKID": {
                "key": "",
                "type": "NVARCHAR",
                "length": 5,
                "scale": null
            },
            "XPORE": {
                "key": "",
                "type": "NVARCHAR",
                "length": 1,
                "scale": null
            },
            "QSZNR": {
                "key": "",
                "type": "NVARCHAR",
                "length": 10,
                "scale": null
            },
            "QSZDT": {
                "key": "",
                "type": "NVARCHAR",
                "length": 8,
                "scale": null
            },
            "QSSKZ": {
                "key": "",
                "type": "NVARCHAR",
                "length": 2,
                "scale": null
            },
            "BLNKZ": {
                "key": "",
                "type": "NVARCHAR",
                "length": 2,
                "scale": null
            },
            "MINDK": {
                "key": "",
                "type": "NVARCHAR",
                "length": 3,
                "scale": null
            },
            "ALTKN": {
                "key": "",
                "type": "NVARCHAR",
                "length": 10,
                "scale": null
            },
            "ZGRUP": {
                "key": "",
                "type": "NVARCHAR",
                "length": 2,
                "scale": null
            },
            "MGRUP": {
                "key": "",
                "type": "NVARCHAR",
                "length": 2,
                "scale": null
            },
            "UZAWE": {
                "key": "",
                "type": "NVARCHAR",
                "length": 2,
                "scale": null
            },
            "QSREC": {
                "key": "",
                "type": "NVARCHAR",
                "length": 2,
                "scale": null
            },
            "QSBGR": {
                "key": "",
                "type": "NVARCHAR",
                "length": 1,
                "scale": null
            },
            "QLAND": {
                "key": "",
                "type": "NVARCHAR",
                "length": 3,
                "scale": null
            },
            "XEDIP": {
                "key": "",
                "type": "NVARCHAR",
                "length": 1,
                "scale": null
            },
            "FRGRP": {
                "key": "",
                "type": "NVARCHAR",
                "length": 4,
                "scale": null
            },
            "TOGRR": {
                "key": "",
                "type": "NVARCHAR",
                "length": 4,
                "scale": null
            },
            "TLFXS": {
                "key": "",
                "type": "NVARCHAR",
                "length": 31,
                "scale": null
            },
            "INTAD": {
                "key": "",
                "type": "NVARCHAR",
                "length": 130,
                "scale": null
            },
            "XLFZB": {
                "key": "",
                "type": "NVARCHAR",
                "length": 1,
                "scale": null
            },
            "GUZTE": {
                "key": "",
                "type": "NVARCHAR",
                "length": 4,
                "scale": null
            },
            "GRICD": {
                "key": "",
                "type": "NVARCHAR",
                "length": 2,
                "scale": null
            },
            "GRIDT": {
                "key": "",
                "type": "NVARCHAR",
                "length": 2,
                "scale": null
            },
            "XAUSZ": {
                "key": "",
                "type": "NVARCHAR",
                "length": 1,
                "scale": null
            },
            "CERDT": {
                "key": "",
                "type": "NVARCHAR",
                "length": 8,
                "scale": null
            },
            "CONFS": {
                "key": "",
                "type": "NVARCHAR",
                "length": 1,
                "scale": null
            },
            "UPDAT": {
                "key": "",
                "type": "NVARCHAR",
                "length": 8,
                "scale": null
            },
            "UPTIM": {
                "key": "",
                "type": "NVARCHAR",
                "length": 6,
                "scale": null
            },
            "NODEL": {
                "key": "",
                "type": "NVARCHAR",
                "length": 1,
                "scale": null
            },
            "TLFNS": {
                "key": "",
                "type": "NVARCHAR",
                "length": 30,
                "scale": null
            },
            "J_SC_SUBCONTYPE": {
                "key": "",
                "type": "NVARCHAR",
                "length": 1,
                "scale": null
            },
            "J_SC_COMPDATE": {
                "key": "",
                "type": "NVARCHAR",
                "length": 3,
                "scale": null
            },
            "J_SC_OFFSM": {
                "key": "",
                "type": "NVARCHAR",
                "length": 1,
                "scale": null
            },
            "J_SC_OFFSR": {
                "key": "",
                "type": "NVARCHAR",
                "length": 3,
                "scale": null
            },
            "BASIS_PNT": {
                "key": "",
                "type": "DECIMAL",
                "length": 6,
                "scale": 3
            },
            "GMVKZK": {
                "key": "",
                "type": "NVARCHAR",
                "length": 1,
                "scale": null
            },
            "PREPAY_RELEVANT": {
                "key": "",
                "type": "NVARCHAR",
                "length": 1,
                "scale": null
            },
            "ASSIGN_TEST": {
                "key": "",
                "type": "NVARCHAR",
                "length": 4,
                "scale": null
            },
            "AVSND": {
                "key": "",
                "type": "NVARCHAR",
                "length": 1,
                "scale": null
            },
            "AD_HASH": {
                "key": "",
                "type": "NVARCHAR",
                "length": 10,
                "scale": null
            },
            "CVP_XBLCK_B": {
                "key": "",
                "type": "NVARCHAR",
                "length": 1,
                "scale": null
            },
            "CIIUCODE": {
                "key": "",
                "type": "NVARCHAR",
                "length": 4,
                "scale": null
            },
            "FORN_FICHA": {
                "key": "",
                "type": "NVARCHAR",
                "length": 1,
                "scale": null
            },
            "EXP_POLIT": {
                "key": "",
                "type": "NVARCHAR",
                "length": 1,
                "scale": null
            },
            "DATA_INICIO": {
                "key": "",
                "type": "NVARCHAR",
                "length": 8,
                "scale": null
            },
            "DATA_FIM": {
                "key": "",
                "type": "NVARCHAR",
                "length": 8,
                "scale": null
            }
        },
        "download_dir": "/Users/osuser/test/data/LFB1",
        "download_mode": "local"
    }
]
```

</details>

And then we can execute the download with the command:
```bash
python HANA_downloader.py "download" --config_dir "~/test/configuration/LFB1"
```

As a result we make a `ls -lh` on the direcotry `~/test/data/LFB1`
```bash
total 10024
-rw-r--r--  1 osuser  staff   1.7M Mar 14 15:59 data_ECC_LFB1_20230314155914472378.parquet.snappy
-rw-r--r--  1 osuser  staff   1.6M Mar 14 16:00 data_ECC_LFB1_20230314160040984914.parquet.snappy
-rw-r--r--  1 osuser  staff   1.6M Mar 14 16:02 data_ECC_LFB1_20230314160214780727.parquet.snappy
```
The result size endded being less than predicted due to the compression

# Example batch download in several files group by fields

Next we will take as an example to process batch of files of the table BSEG grouped by years

We can check the total row count and size of the table with the next statement on the DB:
```sql
select * from sys.m_tables where table_name in('BSEG');
```
As a result will get the outut:
| SCHEMA_NAME | TABLE_NAME | RECORD_COUNT | TABLE_SIZE  | IS_COLUMN_TABLE | TABLE_TYPE | IS_PARTITIONED | IS_REPLICATED |
|-------------|------------|--------------|-------------|-----------------|------------|----------------|---------------|
| ECC         | BSEG       | 1056273936   | 64562127561 | TRUE            | COLUMN     | FALSE          | FALSE         |

The total size of the table is __64,5 GB__

The premise is to group files data by year.

To calculate how much records we have by every year we execute:
```sql
SELECT GJAHR, COUNT(*) FROM ECC.BSEG GROUP BY GJAHR ORDER BY GJAHR;
```
With the next output:

| GJAHR | COUNT(*)  |
|-------|-----------|
| 2004  | 15        |
| 2005  | 88        |
| 2006  | 255       |
| 2007  | 3162      |
| 2008  | 6394      |
| 2009  | 3061      |
| 2010  | 3716      |
| 2011  | 6398      |
| 2012  | 12996     |
| 2013  | 21737     |
| 2014  | 27147     |
| 2015  | 49688     |
| 2016  | 100619    |
| 2017  | 3181217   |
| 2018  | 48319135  |
| 2019  | 240777059 |
| 2020  | 229203936 |
| 2021  | 243034063 |
| 2022  | 244467720 |
| 2023  | 47055530  |

Aproximatly from 2019 to 2022 each year have a footprint of _14 GB_ on the database

So we will first create the configuration files, and each file limit to a max of 1 GB:
```bash
python HANA_downloader.py "configure" --config_dir "~/test/configuration/BSEG" \
--table "BSEG" --table_schema "ECC" --group GJAHR \
--null_treatment "/path_to_json/null.json" \
--limit_mode "GB" --limit_num 1
--download_dir "~/test/data/BSEG" --download_mode local
```

Example of the result json file [here](/samples/config_ECC_BSEG_GJAHR-2022.json)

As a result we will get the next list of configuration files:
```
config_ECC_BSEG_GJAHR-2004.json config_ECC_BSEG_GJAHR-2014.json
config_ECC_BSEG_GJAHR-2005.json config_ECC_BSEG_GJAHR-2015.json
config_ECC_BSEG_GJAHR-2006.json config_ECC_BSEG_GJAHR-2016.json
config_ECC_BSEG_GJAHR-2007.json config_ECC_BSEG_GJAHR-2017.json
config_ECC_BSEG_GJAHR-2008.json config_ECC_BSEG_GJAHR-2018.json
config_ECC_BSEG_GJAHR-2009.json config_ECC_BSEG_GJAHR-2019.json
config_ECC_BSEG_GJAHR-2010.json config_ECC_BSEG_GJAHR-2020.json
config_ECC_BSEG_GJAHR-2011.json config_ECC_BSEG_GJAHR-2021.json
config_ECC_BSEG_GJAHR-2012.json config_ECC_BSEG_GJAHR-2022.json
config_ECC_BSEG_GJAHR-2013.json config_ECC_BSEG_GJAHR-2023.json
```

We also added a manipulation of the fields of type DATS to be converted if not match a date format to be '19000101', but if only have zero or spaces we leave it only with a space.
So in that case we override any error, but if the value is an ABAP null we clean the string.
More details [here](/samples/example_null_treatment_several_DATS_fields.json)
