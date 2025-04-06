# Contributing Guide

## How to Contribute to this Project

1. Clone this repository:

    ```bash
    git clone https://github.com/pycontw/pycon-etl
    ```

2. Create a new branch:

    ```bash
    git checkout -b <branch-name>
    ```

3. Make your changes.

    > **NOTICE:** We are still using Airflow v1, so please read the official document [Apache Airflow v1.10.15 Documentation](https://airflow.apache.org/docs/apache-airflow/1.10.15/) to ensure your changes are compatible with our current version.

    If your task uses an external service, add the connection and variable in the Airflow UI.

4. Test your changes in your local environment:

    - Ensure the DAG file is loaded successfully.
    - Verify that the task runs successfully.
    - Confirm that your code is correctly formatted and linted.
    - Check that all necessary dependencies are included in `requirements.txt`.

5. Push your branch:

    ```bash
    git push origin <branch-name>
    ```

6. Create a Pull Request (PR).

7. Wait for the review and merge.

8. Write any necessary documentation.

## Release Management

Please use [GitLab Flow](https://about.gitlab.com/topics/version-control/what-is-gitlab-flow/); otherwise, you cannot pass Docker Hub CI.

## Dependency Management

Airflow dependencies are managed by [uv]. For more information, refer to the [Airflow Installation Documentation](https://airflow.apache.org/docs/apache-airflow/1.10.15/installation.html).

## Code Convention

### Airflow DAG

- Please refer to [this article](https://medium.com/@davidtnfsh/%E5%A4%A7%E6%95%B0%E6%8D%AE%E4%B9%8B%E8%B7%AF-%E9%98%BF%E9%87%8C%E5%B7%B4%E5%B7%B4%E5%A4%A7%E6%95%B0%E6%8D%AE%E5%AE%9E%E8%B7%B5-%E8%AE%80%E6%9B%B8%E5%BF%83%E5%BE%97-54e795c2b8c) for naming guidelines.

  - Examples:
    1. `ods/opening_crawler`: Crawlers written by @Rain. These openings can be used for the recruitment board, which was implemented by @tai271828 and @stacy.
    2. `ods/survey_cake`: A manually triggered uploader that uploads questionnaires to BigQuery. The uploader should be invoked after we receive the SurveyCake questionnaire.

- Table name convention:
  ![img](https://miro.medium.com/max/1400/1*bppuEKMnL9gFnvoRHUO8CQ.png)

### Format

Please use `make format` to format your code before committing, otherwise, the CI will fail.

### Commit Message

It is recommended to use [Commitizen](https://commitizen-tools.github.io/commitizen/).

### CI/CD

Please check the [.github/workflows](.github/workflows) directory for details.

[uv]: https://docs.astral.sh/uv/