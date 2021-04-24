FROM mcr.microsoft.com/dotnet/sdk:5.0-alpine
RUN wget https://github.com/makiuchi-d/arelo/releases/download/v1.4/arelo_linux_x86_64.tar.gz && \
  tar -C /usr/local/bin -xzf arelo_linux_x86_64.tar.gz && \
  rm arelo_linux_x86_64.tar.gz

WORKDIR /app
COPY . .
RUN dotnet build --configuration Release
ENTRYPOINT [ "dotnet", "/app/bin/Release/net5.0/api.dll" ]