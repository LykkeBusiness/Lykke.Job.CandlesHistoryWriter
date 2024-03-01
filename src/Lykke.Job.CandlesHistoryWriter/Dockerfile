FROM mcr.microsoft.com/dotnet/aspnet:6.0-alpine
WORKDIR /app
COPY . .
RUN apk add --no-cache icu-libs \
&& apk add --no-cache tzdata \ cp cert/*.crt /usr/local/share/ca-certificates/ 2>/dev/null \
&& update-ca-certificates \
|| echo "No specific certificate given."
RUN apk update && apk add bash
ENV DOTNET_SYSTEM_GLOBALIZATION_INVARIANT=false
ENV COMPlus_DbgEnableMiniDump=1
ENV COMPlus_CreateDumpDiagnostics=1
ENV COMPlus_EnableCrashReport=1
ENV COMPlus_DbgMiniDumpType=4
ENV COMPlus_DbgMiniDumpName="crashdump.dmp"
ENTRYPOINT ["dotnet", "Lykke.Job.CandlesHistoryWriter.dll"]
