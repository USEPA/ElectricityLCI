# Welcome to ElectricityLCI <!-- omit in toc -->

In this guide you will get an overview of the contribution workflow from opening an issue, creating a PR, reviewing, and merging the PR.

## Getting started

To get an overview of the project, read the [README](README.md).
Here are some resources to help you get started with contributions:

- [Set up Git](https://docs.github.com/en/get-started/quickstart/set-up-git)
- [GitHub flow](https://docs.github.com/en/get-started/quickstart/github-flow)
- [Collaborating with pull requests](https://docs.github.com/en/github/collaborating-with-pull-requests)
- [Contributing to projects](https://docs.github.com/en/get-started/quickstart/contributing-to-projects)


## Contributing workflow

The general workflow for contributing is as follows:

1. Fork repository (see [here](https://docs.github.com/en/get-started/quickstart/contributing-to-projects#forking-a-repository) for details).
  - Using GitHub Desktop:
    - [Getting started with GitHub Desktop](https://docs.github.com/en/desktop/installing-and-configuring-github-desktop/getting-started-with-github-desktop) will guide you through setting up Desktop.
    - Once Desktop is set up, you can use it to [fork the repo](https://docs.github.com/en/desktop/contributing-and-collaborating-using-github-desktop/cloning-and-forking-repositories-from-github-desktop)!
  - Using the command line:
    - [Fork the repo](https://docs.github.com/en/github/getting-started-with-github/fork-a-repo#fork-an-example-repository) so that you can make your changes without affecting the original project until you're ready to merge them.
2. Create an issue on the original repository site ([here](https://github.com/USEPA/ElectricityLCI/issues)).
3. Create a new branch in your forked version of the repo based on the latest pull on development branch.
  Name the branch after the issue (e.g., using the issue number, issue68, or topically, add-contrib).

  ```sh
  git pull --all
  git checkout --track origin/development
  git pull
  git checkout -b BRANCH-NAME
  ```

4. Push the branch to the remote in your forked repo.

  ```sh
  git push -u origin BRANCH-NAME
  ```
5. Create a pull request (see [here](https://docs.github.com/en/pull-requests/collaborating-with-pull-requests/proposing-changes-to-your-work-with-pull-requests/creating-a-pull-request) for details) using the development branch as the base and your new branch as the head.
6. Link the pull request to the issue using the Development option (see [here](https://docs.github.com/en/issues/tracking-your-work-with-issues/linking-a-pull-request-to-an-issue) for details); note that this only works for users with write capabilities on the original repo.
7. Use GitHub pull request review and discussion space to vet changes before they are merged into the development branch.
8. Once development branch reaches a certain milestone, merge into master as a new release.


## The Pull Request

When you're finished with the changes, create a pull request, also known as a PR.

- Fill the "Ready for review" template so that we can review your PR.
  This template helps reviewers understand your changes as well as the purpose of your pull request.
- Don't forget to [link PR to issue](https://docs.github.com/en/issues/tracking-your-work-with-issues/linking-a-pull-request-to-an-issue) if you are solving one.
- Enable the checkbox to [allow maintainer edits](https://docs.github.com/en/github/collaborating-with-issues-and-pull-requests/allowing-changes-to-a-pull-request-branch-created-from-a-fork) so the branch can be updated for a merge.

Once you submit your PR, a team member will review your proposal.
We may ask questions or request additional information.

- We may ask for changes to be made before a PR can be merged, either using [suggested changes](https://docs.github.com/en/github/collaborating-with-issues-and-pull-requests/incorporating-feedback-in-your-pull-request) or pull request comments.
  You can apply suggested changes directly through the UI. You can make any other changes in your fork, then commit them to your branch.
- As you update your PR and apply changes, mark each conversation as [resolved](https://docs.github.com/en/github/collaborating-with-issues-and-pull-requests/commenting-on-a-pull-request#resolving-conversations).
- If you run into any merge issues, checkout this [git tutorial](https://github.com/skills/resolve-merge-conflicts) to help you resolve merge conflicts and other issues.
