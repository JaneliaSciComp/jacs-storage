# Exit after any error
set -o errexit
VER=$1

if [ "${VER}" == "" ]; then
    echo "Usage: release.sh <version>"
    exit 1
fi

git merge dev

mv build.gradle build.tmp.prerelease

sed s/"version = '${VER}.SNAPSHOT'"/"version = '${VER}.RELEASE'"/ build.tmp.prerelease > build.gradle

git add build.gradle
git commit -m "Prepare ${VER}.RELEASE"
git push origin

# tag it
git tag "${VER}.RELEASE"
git push --tags

rm -f build.tmp.prerelease
