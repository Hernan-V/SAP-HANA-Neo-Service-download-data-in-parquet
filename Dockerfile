FROM python:3.9.0-slim

SHELL ["/bin/bash","-l","-c"]

# Arguments for Neo Tunnel could be set at build or run
ARG BTP_SUBACCOUNT_HOST=
ENV BTP_SUBACCOUNT_HOST=$BTP_SUBACCOUNT_HOST
ARG BTP_SUBACCOUNT_USER=
ENV BTP_SUBACCOUNT_USER=$BTP_SUBACCOUNT_USER
ARG BTP_SUBACCOUNT_PASSWORD=
ENV BTP_SUBACCOUNT_PASSWORD=$BTP_SUBACCOUNT_PASSWORD
ARG BTP_SUBACCOUNT_ID=
ENV BTP_SUBACCOUNT_ID=$BTP_SUBACCOUNT_ID
ARG BTP_SUBACCOUNT_DB=
ENV BTP_SUBACCOUNT_DB=$BTP_SUBACCOUNT_DB

# Enviroment varibales for HANA DB login
ENV SAP_HANA_HOST=
ENV SAP_HANA_PORT=
ENV SAP_HANA_USER=
ENV SAP_HANA_PASSWORD=

# Install any necessary dependencies
RUN apt-get update && \
    apt-get install -y \
        curl \
        unzip && \
    rm -rf /var/lib/apt/lists/*

# Download JVM from SAP repository
WORKDIR /sapjvm
RUN curl -fsSL 'https://tools.eu1.hana.ondemand.com/additional/sapjvm-8.1.092-linux-x64.zip' -H 'User-Agent: Mozilla/5.0 (X11; Ubuntu; Linux x86_64; rv:109.0) Gecko/20100101 Firefox/111.0' -H 'Accept: text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,*/*;q=0.8' -H 'Accept-Language: en-US,en;q=0.5' -H 'Accept-Encoding: gzip, deflate, br' -H 'DNT: 1' -H 'Connection: keep-alive' -H 'Referer: https://tools.eu1.hana.ondemand.com/' -H 'Cookie: eula_3_1_agreed=tools.hana.ondemand.com/developer-license-3_1.txt' -H 'Upgrade-Insecure-Requests: 1' -H 'Sec-Fetch-Dest: document' -H 'Sec-Fetch-Mode: navigate' -H 'Sec-Fetch-Site: same-origin' -H 'Sec-Fetch-User: ?1' -o sapjvm.zip

# Uncompress the file and delete the zip
RUN unzip -q sapjvm.zip
RUN rm sapjvm.zip

# Add path for the Java Virtual Machine
RUN echo "export PATH=\"\$PATH:/sapjvm/sapjvm_8/jre\"" >> $HOME/.bashrc
RUN echo "export JAVA_HOME=/sapjvm/sapjvm_8/jre" >> $HOME/.bashrc

# Download SAP BTP Neo SDK
WORKDIR /neosdk
RUN curl -fsSL 'https://tools.hana.ondemand.com/sdk/neo-java-web-sdk-3.176.4.zip' -H 'User-Agent: Mozilla/5.0 (X11; Ubuntu; Linux x86_64; rv:109.0) Gecko/20100101 Firefox/111.0' -H 'Accept: text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,*/*;q=0.8' -H 'Accept-Language: en-US,en;q=0.5' -H 'Accept-Encoding: gzip, deflate, br' -H 'Connection: keep-alive' -H 'Referer: https://tools.eu1.hana.ondemand.com/' -H 'Cookie: eula_3_1_agreed=tools.hana.ondemand.com/developer-license-3_1.txt' -H 'Upgrade-Insecure-Requests: 1' -H 'Sec-Fetch-Dest: document' -H 'Sec-Fetch-Mode: navigate' -H 'Sec-Fetch-Site: same-origin' -H 'Sec-Fetch-User: ?1' -o neosdk.zip

# Uncompress the file and delete the zip
RUN unzip -q neosdk.zip
RUN rm neosdk.zip
# Add to path
RUN echo "export PATH=\"\$PATH:/neosdk/tools\"" >> $HOME/.bashrc

WORKDIR /app

COPY . /app

# Install any necessary Python packages
RUN pip install --no-cache-dir -r requirements.txt

CMD python HANA-downloader.py
ENTRYPOINT neo.sh open-db-tunnel -h $BTP_SUBACCOUNT_HOST -u $BTP_SUBACCOUNT_USER \
-p $BTP_SUBACCOUNT_PASSWORD -a $BTP_SUBACCOUNT_ID -i $BTP_SUBACCOUNT_DB --background
