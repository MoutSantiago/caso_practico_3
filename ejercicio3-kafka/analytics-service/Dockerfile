FROM mcr.microsoft.com/dotnet/sdk:8.0 AS build
WORKDIR /src
COPY AnalyticsService.csproj ./
RUN dotnet restore
COPY . .
RUN dotnet publish -c Release -o /app --no-restore -p:UseAppHost=false

# Debian (no Alpine): librdkafka, la librería nativa de Confluent.Kafka, se distribuye para glibc.
FROM mcr.microsoft.com/dotnet/aspnet:8.0
WORKDIR /app
COPY --from=build /app ./
EXPOSE 3003
ENTRYPOINT ["dotnet", "AnalyticsService.dll"]
