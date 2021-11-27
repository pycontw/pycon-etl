# Contrib

## Upload KKTIX

## Survey Cake

[Demo Video](https://www.loom.com/share/4c494f1d3ce443c6a43ed514c53b70ff)
1. download CSV from survey cake (account: data-strategy-registration-survey-cake@pycon.tw)
2. `. ./.env.sh `
2. `cd contrib/survey_cake`
3. `python upload-survey-cake-csv-to-bigquery.py --year=<20xx> -c <name of contributor>`
    1. it would upload data to Bigquery's `test` dataset
    2. If everything looks good, you can `copy` the `fact table` and `dimension table` first
    3. Then run `python upload-survey-cake-csv-to-bigquery.py --year=<20xx> -p`. `-p` stands for `production`