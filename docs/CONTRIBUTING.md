# How to Contribute to HugeGraph

> Refer [website-doc](https://hugegraph.apache.org/docs/contribution-guidelines/) for the latest information.

Thanks for taking the time to contribute!
As an open source project, HugeGraph is looking forward to being contributed from everyone, and we are also grateful to all the contributors.

The following is a contribution guide for HugeGraph:

<img width="884" alt="image" src="https://user-images.githubusercontent.com/9625821/159643158-8bf72c0a-93c3-4a58-8912-7b2ab20ced1d.png">

## 1. Preparation

**Recommended**: You can use [GitHub desktop](https://desktop.github.com/) to greatly simplify the PR process.

We can contribute by reporting issues, submitting code patches or any other feedback.

Before submitting the code, we need to do some preparation:

1. Sign up or login to GitHub: [https://github.com](https://github.com)

2. Fork HugeGraph repo from GitHub: [https://github.com/apache/hugegraph/fork](https://github.com/apache/hugegraph/fork)

3. Clone code from fork repo to local: [https://github.com/${GITHUB_USER_NAME}/hugegraph](https://github.com/${GITHUB_USER_NAME}/hugegraph)

   ```shell
   # clone code from remote to local repo
   git clone https://github.com/${GITHUB_USER_NAME}/hugegraph.git hugegraph
   ```

4. Configure local HugeGraph repo

   ```shell
   cd hugegraph

   # add upstream to synchronize the latest code
   git remote add hugegraph https://github.com/apache/hugegraph

   # set name and email to push code to github
   git config user.name "{full-name}" # like "Jermy Li"
   git config user.email "{email-address-of-github}" # like "jermy@apache.org"
   ```

## 2. Create an Issue on GitHub

If you encounter bugs or have any questions, please go to [GitHub Issues](https://github.com/apache/hugegraph/issues) to report them and feel free to [create an issue](https://github.com/apache/hugegraph/issues/new).

## 3. Make changes of code locally

#### 3.1 Create a new branch

Please don't use master branch for development. Instead, we should create a new branch:

```shell
# checkout master branch
git checkout master
# pull the latest code from official hugegraph
git pull hugegraph
# create new branch: bugfix-branch
git checkout -b bugfix-branch
```

#### 3.2 Change the code

Assume that we need to modify some files like "HugeGraph.java" and "HugeFactory.java":

```shell
# modify code to fix a bug
vim hugegraph-server/hugegraph-core/src/main/java/org/apache/hugegraph/HugeGraph.java
vim hugegraph-server/hugegraph-core/src/main/java/org/apache/hugegraph/HugeFactory.java
# run test locally (optional)
mvn test -Pcore-test,memory
```
Note: Code style is defined by the `.editorconfig` file at the repository root. Checkstyle rules are defined in `style/checkstyle.xml`. Configure your IDE accordingly.

##### 3.2.1 Check licenses
If we want to add new third-party dependencies to the `HugeGraph` project, we need to do the following things:
1. Find the third-party dependent repository, put the dependent `license` file into [./install-dist/release-docs/licenses/](https://github.com/apache/hugegraph/tree/master/install-dist/release-docs/licenses) path.
2. Declare the dependency in [./install-dist/release-docs/LICENSE](https://github.com/apache/hugegraph/blob/master/install-dist/release-docs/LICENSE) `LICENSE` information.
3. Find the NOTICE file in the repository and append it to [./install-dist/release-docs/NOTICE](https://github.com/apache/hugegraph/blob/master/install-dist/release-docs/NOTICE) file (skip this step if there is no NOTICE file).
4. Execute locally [./install-dist/scripts/dependency/regenerate_known_dependencies.sh](https://github.com/apache/hugegraph/blob/master/install-dist/scripts/dependency/regenerate_known_dependencies.sh) to update the dependency list [known-dependencies.txt](https://github.com/apache/hugegraph/blob/master/install-dist/scripts/dependency/known-dependencies.txt) (or manually update).

**Example**: A new third-party dependency is introduced into the project -> `ant-1.9.1.jar`
- The project source code is located at: https://github.com/apache/ant/tree/rel/1.9.1
- LICENSE file: https://github.com/apache/ant/blob/rel/1.9.1/LICENSE
- NOTICE file: https://github.com/apache/ant/blob/rel/1.9.1/NOTICE

The license information of `ant-1.9.1.jar` needs to be specified in the LICENSE file, and the notice information needs to be specified in the NOTICE file. The detailed LICENSE file corresponding to ant-1.9.1.jar needs to be copied to our licenses/ directory. Finally, update the known-dependencies.txt file.

#### 3.3 Commit changes to git repo

After the code has been completed, we submit them to the local git repo:

```shell
# add files to local git index
git add hugegraph-server/hugegraph-core/src/main/java/org/apache/hugegraph/HugeGraph.java
git add hugegraph-server/hugegraph-core/src/main/java/org/apache/hugegraph/HugeFactory.java
# commit to local git repo
git commit
```

Please edit the commit message after running `git commit`, we can explain what and how to fix a bug or implement a feature, the following is an example:

```sh
Fix bug: run deploy multiple times 

fix #ISSUE_ID
```

>  Please remember to fill in the issue id, which GitHub generated after issue creation.

#### 3.4 Push commit to GitHub fork repo

Push the local commit to GitHub fork repo:

```shell
# push the local commit to fork repo
git push origin bugfix-branch:bugfix-branch
```

Note that since GitHub requires submitting code through `username + token` (instead of using `username + password` directly), you need to create a GitHub token from https://github.com/settings/tokens:
<img width="1280" alt="image" src="https://user-images.githubusercontent.com/9625821/163524204-7fe0e6bf-9c8b-4b1a-ac65-6a0ac423eb16.png">

## 4. Create a Pull Request

Go to the web page of GitHub fork repo, there would be a chance to create a Pull Request after pushing to a new branch, click the button "Compare & pull request" to do it. Then edit the description for proposed changes, which can just be copied from the commit message.

Note: please make sure the email address you used to submit the code is bound to the GitHub account. For how to bind the email address, please refer to https://github.com/settings/emails:
<img width="1280" alt="image" src="https://user-images.githubusercontent.com/9625821/163522445-2a50a72a-dea2-434f-9868-3a0d40d0d037.png">

## 5. Code review

 Maintainers will start the code review after all the **automatic** checks are passed:

- Check: Contributor License Agreement is signed
- Check: Travis CI builds are passed (automatically Test and Deploy)

The commit will be accepted and merged if there is no problem after review.

Please click on "Details" to find the problem if any check does not pass.

If there are checks not passed or changes requested, then continue to modify the code and push again.

## 6. Further changes after review 

If we have not passed the review, don't be discouraged. Usually a commit needs to be reviewed several times before being accepted! Please follow the review comments and make further changes.

After the further changes, we submit them to the local repo:

```shell
# commit all updated files in a new commit,
# please feel free to enter any appropriate commit message, note that
# we will squash all commits in the pull request as one commit when
# merging into the master branch.
git commit -a
```

> If there are conflicts that prevent the code from being merged, we need to rebase on master branch:
>
> ```shell
> # synchronize the latest code
> git checkout master
> git pull hugegraph
> # rebase on master
> git checkout bugfix-branch
> git rebase -i master
> ```

And push it to GitHub fork repo again:

```shell
# force push the local commit to fork repo
git push -f origin bugfix-branch:bugfix-branch
```

GitHub will automatically update the Pull Request after we push it, wait for code review.

For Any question, please contact us through [dev@hugegraph.apache.org](mailto:dev@hugegraph.apache.org) ([subscriber](https://hugegraph.apache.org/docs/contribution-guidelines/subscribe/) only)


