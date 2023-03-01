echo "--- System dependencies"

PYTHON_VERSION="$1"
sudo add-apt-repository --yes ppa:deadsnakes/ppa
sudo apt-get update && sudo apt-get install -y \
    "python${PYTHON_VERSION}" "python${PYTHON_VERSION}-dev" "python${PYTHON_VERSION}-venv" \
    git make jq docker \
    openjdk-17-jdk-headless openjdk-11-jdk-headless \
    zlib1g zlib1g-dev libssl-dev libbz2-dev libsqlite3-dev
export JAVA11_HOME=/usr/lib/jvm/java-11-openjdk-amd64
export JAVA17_HOME=/usr/lib/jvm/java-17-openjdk-amd64

echo "--- Run IT test :pytest:"

export RALLY_HOME=$HOME
"python${PYTHON_VERSION}" -m venv .venv
source .venv/bin/activate

pip install nox
nox -s "it-${PYTHON_VERSION}"
