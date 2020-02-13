@echo off

set DEBUG_MODE=

if "%1" == "debug" (
  set DEBUG_MODE=debug
)

cd com.ingeint.kafka.targetplatform
call .\plugin-builder.bat %DEBUG_MODE% ..\com.ingeint.kafka ..\com.ingeint.kafka.test
cd ..
