# El contexto de build es la carpeta ejercicio1-rest (para poder copiar /proto).
# El SDK es Debian (no Alpine) porque Grpc.Tools trae un protoc compilado contra glibc.
FROM mcr.microsoft.com/dotnet/sdk:8.0 AS build
WORKDIR /src
COPY proto ./proto
COPY inventory-service/InventoryService.csproj inventory-service/
RUN dotnet restore inventory-service/InventoryService.csproj
COPY inventory-service/ inventory-service/
RUN dotnet publish inventory-service/InventoryService.csproj -c Release -o /app --no-restore -p:UseAppHost=false

FROM mcr.microsoft.com/dotnet/aspnet:8.0-alpine
WORKDIR /app
COPY --from=build /app ./
ENV DOTNET_ENVIRONMENT=Production
EXPOSE 3001 50051
ENTRYPOINT ["dotnet", "InventoryService.dll"]
