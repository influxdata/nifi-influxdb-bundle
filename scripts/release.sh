#
# Licensed to the Apache Software Foundation (ASF) under one or more
# contributor license agreements.  See the NOTICE file distributed with
# this work for additional information regarding copyright ownership.
# The ASF licenses this file to You under the Apache License, Version 2.0
# (the "License"); you may not use this file except in compliance with
# the License.  You may obtain a copy of the License at
#
#      http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
#

#!/usr/bin/env bash

usage() { echo "Usage: $0 -r 1.0 (specify release version) -d 1.1-SNAPSHOT (specify development version)" 1>&2; exit 1; }

prompt_confirm() {
  while true; do
    read -r -n 1 -p "${1:-Continue?} [y/n]: " REPLY
    case $REPLY in
      [yY]) echo ; return 0 ;;
      [nN]) echo ; return 1 ;;
      *) printf " \033[31m %s \n\033[0m" "invalid input"
    esac
  done
}

SCRIPT_PATH="$( cd "$(dirname "$0")" ; pwd -P )"

while getopts ":r:d:" o; do
    case "${o}" in
        r)
            releaseVersion=${OPTARG}
            ;;
        d)
            developmentVersion=${OPTARG}
            ;;
        *)
            usage
            ;;
    esac
done
shift $((OPTIND-1))

if [ -z "${releaseVersion}" ] || [ -z "${developmentVersion}" ]; then
    usage
fi

echo
echo "Preparing release" ${releaseVersion} "with tag v"${releaseVersion} "and next development version" ${developmentVersion} "..."
echo

prompt_confirm "Do you want continue?" || exit 0

cd ${SCRIPT_PATH}/..

#
# Replace flow.xml
#
mvn replacer:replace -DtoVersion=${releaseVersion}

#
# Update POM version
#
mvn org.codehaus.mojo:versions-maven-plugin:2.1:set -DnewVersion=${releaseVersion} -DgenerateBackupPoms=false

git add --all
git commit -am "release nifi-influx-database-bundle-${releaseVersion}"

echo
prompt_confirm "Do you want push release ${releaseVersion}?" || exit 0

git push origin master

#
# Replace flow.xml
#
mvn replacer:replace -DtoVersion=${developmentVersion}

#
# Update POM version
#
mvn org.codehaus.mojo:versions-maven-plugin:2.1:set -DnewVersion=${developmentVersion} -DgenerateBackupPoms=false

echo

echo "Prepared the next development iteration ${developmentVersion}!"
echo
echo "Next steps"
echo
echo "  1. wait for finish build on Travis CI: https://travis-ci.org/bonitoo-io/nifi-influxdb-bundle"
echo "  2. add ${developmentVersion} iteration to CHANGELOG.md"
echo "  3. commit changes: git commit -am \"prepare for next development iteration ${developmentVersion}\""
echo "  4. push changes: git push origin master"
echo "  5. delete old snapshot release from: https://github.com/bonitoo-io/nifi-influxdb-bundle/releases"
echo "  6. delete old snapshot tag: git push --delete origin v${releaseVersion}-SNAPSHOT"
