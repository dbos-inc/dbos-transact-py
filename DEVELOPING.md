### Creating a Release

To cut a new release, run:

```shell
python3 make_release.py [--version_number <version>]
```

Version numbers follow [semver](https://semver.org/).
This command tags the latest commit with the version number and creates a release branch for it.
If a version number is not supplied, it automatically generated a version number by incrementing the last released minor version.

### Patching a release 

To patch a release, push the patch as a commit to the appropriate release branch.
Then, tag it with a version number:

```shell
git tag <version-number>
git push --tags
```

This version must follow semver: It should increment by one the patch number of the release branch.

### Preview Versions

Preview versions are [PEP440](https://peps.python.org/pep-0440/)-compliant alpha versions.
They can be published from `main`.
Their version number is `<next-release-version>a<number-of-git-commits-since-release>`.
You can install the latest preview version with `pip install --pre dbos`.

### Test Versions

Test versions are built from feature branches.
Their version number is `<next-release-version>a<number-of-git-commits-since-release>+<git-hash>`.

### Publishing

TODO: Add a GHA to publish from a branch.
