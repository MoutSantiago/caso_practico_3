# El contexto de build es la carpeta ejercicio1-rest (para poder copiar /proto).
# El SDK es Debian (no Alpine) porque Grpc.Tools trae un protoc compilado contra glibc.
FROM mcr.microsoft.com/dotnet/sdk:8.0 AS build
WORKDIR /src
COPY proto ./proto
COPY orders-service/OrdersService.csproj orders-service/
RUN dotnet restore orders-service/OrdersService.csproj
COPY orders-service/ orders-service/
RUN dotnet publish orders-service/OrdersService.csproj -c Release -o /app --no-restore -p:UseAppHost=false

FROM mcr.microsoft.com/dotnet/aspnet:8.0-alpine
WORKDIR /app
COPY --from=build /app ./
EXPOSE 3000
ENTRYPOINT ["dotnet", "OrdersService.dll"]
