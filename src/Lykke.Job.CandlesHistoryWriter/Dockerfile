FROM mcr.microsoft.com/dotnet/aspnet:6.0
WORKDIR /app
COPY . .
RUN cp cert/*.crt /usr/local/share/ca-certificates/ 2>/dev/null \
&& update-ca-certificates \
|| echo "No specific certificate given."
ENV COMPlus_DbgEnableMiniDump=1
ENV COMPlus_CreateDumpDiagnostics=1
ENV COMPlus_EnableCrashReport=1
ENV COMPlus_DbgMiniDumpType=4
ENV COMPlus_DbgMiniDumpName="crashdump.dmp"
ENTRYPOINT ["dotnet", "Lykke.Job.CandlesHistoryWriter.dll"]
