# Contributing Guide

## How to Contribute

1. Clone this repository:

```bash
git clone https://github.com/pycontw/pycon-etl
```

2. Create a new branch:

```bash
git checkout -b <branch-name>
```

3. Make your changes.

   > **NOTICE:** We are still using Airflow v1, so please read the official document [Apache Airflow v1.10.13 Documentation](https://airflow.apache.org/docs/apache-airflow/1.10.13/) to make sure your changes are compatible with our current version.

   If your task uses an external service, add the connection and variable in the Airflow UI.

4. Test your changes in your local environment:

   - Test that the DAG file is loaded successfully.
   - Test that the task is running successfully.
   - Ensure your code is formatted and linted correctly.
   - Check whether the necessary dependencies are included in `requirements.txt`.

5. Push your branch:

   ```bash
   git push origin <branch-name>
   ```

6. Create a Pull Request (PR).

7. Wait for the review and merge.

8. Write any necessary documentation.

## Release Management

Please use [GitLab Flow](https://about.gitlab.com/topics/version-control/what-is-gitlab-flow/), otherwise, you cannot pass docker hub CI

## Dependency Management

Please use poetry to manage dependencies

```bash
poetry add <package>
poetry remove <package>
```

If you are using a new package, please update `requirements.txt` by running `make deps`.

## Code Convention

### Airflow DAG

- Please refer to [this article](https://medium.com/@davidtnfsh/%E5%A4%A7%E6%95%B0%E6%8D%AE%E4%B9%8B%E8%B7%AF-%E9%98%BF%E9%87%8C%E5%B7%B4%E5%B7%B4%E5%A4%A7%E6%95%B0%E6%8D%AE%E5%AE%9E%E8%B7%B5-%E8%AE%80%E6%9B%B8%E5%BF%83%E5%BE%97-54e795c2b8c) for naming guidline

  - examples 1. `ods/opening_crawler`: Crawlers written by @Rain. Those openings can be used for the recruitment board, which was implemented by @tai271828 and @stacy. 2. `ods/survey_cake`: A manually triggered uploader that would upload questionnaires to bigquery. The uploader should be invoked after we receive the surveycake questionnaire.

- table name convention:
  ![img](https://miro.medium.com/max/1400/1*bppuEKMnL9gFnvoRHUO8CQ.png)

### Format

Please use `make format` to format your code before commit, otherwise, the CI will fail.

### Commit Message

Recommended to use [Commitizen](https://commitizen-tools.github.io/commitizen/).

### CI/CD

Please check [.github/workflows](.github/workflows) for details.