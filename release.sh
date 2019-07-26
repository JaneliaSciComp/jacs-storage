# Exit after any error
set -o errexit
VER=$1
# optional next version
NEXT_VER=$2

if [ "${VER}" == "" ]; then
    echo "Usage: release.sh <version> [<next-version>]"
    exit 1
fi

git checkout master
git merge dev

mv build.gradle build.tmp.prerelease

sed s/"version = '${VER}.SNAPSHOT'"/"version = '${VER}.RELEASE'"/ build.tmp.prerelease > build.gradle

git add build.gradle
git commit -m "Prepare ${VER}.RELEASE"
git push origin

# tag it
git tag "${VER}"
git push --tags

rm -f build.tmp.prerelease

if [ "${NEXT_VER}" == "" ]; then
    # done
    exit 0
fi

# otherwise continue
git checkout dev
git rebase master

mv build.gradle build.tmp.presnapshot
sed s/"version = '${VER}.RELEASE'"/"version = '${NEXT_VER}.SNAPSHOT'"/ build.tmp.presnapshot > build.gradle

git add build.gradle
git commit -m "Prepare next build"
git push origin

rm build.tmp.presnapshot
