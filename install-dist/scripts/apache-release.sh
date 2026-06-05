#!/usr/bin/env bash
#
# Licensed to the Apache Software Foundation (ASF) under one or more
# contributor license agreements.  See the NOTICE file distributed with
# this work for additional information regarding copyright ownership.
# The ASF licenses this file to You under the Apache License, Version 2.0
# (the "License"); you may not use this file except in compliance with
# the License.  You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
#

GROUP="hugegraph"
# current repository name
REPO="${GROUP}"
# release version (input by committer)
RELEASE_VERSION=$1
USERNAME=$2
PASSWORD=$3
# git release branch (check it carefully)
GIT_BRANCH="release-${RELEASE_VERSION}"

RELEASE_VERSION=${RELEASE_VERSION:?"Please input the release version behind script"}

WORK_DIR=$(
  cd "$(dirname "$0")" || exit
  pwd
)
cd "${WORK_DIR}" || exit
echo "Current work dir: $(pwd)"

# clean old dir then build a new one
rm -rf dist && mkdir -p dist/apache-${REPO}

# step1: package the source code
cd ../../ && echo "Package source in: $(pwd)"
git archive --format=tar.gz \
  --output="install-dist/scripts/dist/apache-${REPO}/apache-${REPO}-${RELEASE_VERSION}-src.tar.gz" \
  --prefix=apache-${REPO}-"${RELEASE_VERSION}"-src/ "${GIT_BRANCH}" || exit
cd - || exit

# step2: copy the binary file (Optional)
# Note: it's optional for project to generate binary package (skip this step if not need)
cp -v ../../target/apache-${REPO}-"${RELEASE_VERSION}".tar.gz dist/apache-${REPO} || exit

# step3: sign + hash
##### 3.1 sign in source & binary package
gpg --version 1>/dev/null || exit
cd ./dist/apache-${REPO} || exit
for i in *.tar.gz; do
  echo "$i" && eval gpg "${GPG_OPT}" --armor --output "$i".asc --detach-sig "$i"
done

##### 3.2 generate SHA512 file
shasum --version 1>/dev/null || exit
for i in *.tar.gz; do
  echo "$i" && shasum -a 512 "$i" >"$i".sha512
done

#### 3.3 check signature & sha512
for i in *.tar.gz; do
  echo "$i"
  eval gpg "${GPG_OPT}" --verify "$i".asc "$i" || exit
done

for i in *.tar.gz; do
  echo "$i"
  shasum -a 512 --check "$i".sha512 || exit
done

# step4: upload to Apache-SVN
##### 4.1 download apache-SVN
SVN_DIR="${GROUP}-svn-dev"
cd ../
rm -rfv ${SVN_DIR}

svn co "https://dist.apache.org/repos/dist/dev/${GROUP}" ${SVN_DIR}

##### 4.2 copy new release package to svn directory
mkdir -p ${SVN_DIR}/"${RELEASE_VERSION}"
cp -v apache-${REPO}/*tar.gz* "${SVN_DIR}/${RELEASE_VERSION}"
cd ${SVN_DIR} || exit

##### 4.3 check status & add files
svn status
svn add --parents "${RELEASE_VERSION}"/apache-${REPO}-*
## check status again
svn status

##### 4.4 commit & push files
if [ "$USERNAME" = "" ]; then
  svn commit -m "submit files for ${REPO} ${RELEASE_VERSION}" || exit
else
  svn commit -m "submit files for ${REPO} ${RELEASE_VERSION}" --username "${USERNAME}" \
    --password "${PASSWORD}" || exit
fi

echo "Finished all, please check all steps in script manually again!"
