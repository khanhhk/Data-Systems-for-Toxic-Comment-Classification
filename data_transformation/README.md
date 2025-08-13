1.  **Create dbt project**:

```bash
dbt init
```

2. **Manage dependencies**:

Create `packages.yml` into `dbt_nyc` folder:

```txt
packages:
  - package: dbt-labs/dbt_utils
    version: 0.8.0
```

Then run this `dbt deps` to install the packages - by default this directory is ignored by git, to avoid duplicating the source code for the package.

```bash
dbt deps
```

3. **Running dbt project**:

Try running the following commands:

```bash
dbt run
dbt test
```

### Resources:
- Learn more about dbt [in the docs](https://docs.getdbt.com/docs/introduction)
- Check out [Discourse](https://discourse.getdbt.com/) for commonly asked questions and answers
- Join the [chat](https://community.getdbt.com/) on Slack for live discussions and support
- Find [dbt events](https://events.getdbt.com) near you
- Check out [the blog](https://blog.getdbt.com/) for the latest news on dbt's development and best practices
