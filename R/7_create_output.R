
#' Create SUS-SIA/SIH database
#'
#' @description
#' [create_output] Processar arquivos dos sistemas de informação SIA e SIH (DATASUS) e combiná-los com informações do CNES e SIGTAP.
#'
#' @param year_start Ano de início da realização do procedimento
#' @param month_start Mês de início da realização do procedimento
#' @param year_end Ano de término da realização do procedimento
#' @param month_end Mês de término da realização do procedimento
#' @param state_abbr Sigla da Unidade Federativa
#' @param county_id Codigo(s) do Municipio de Atendimento
#' @param health_establishment_id Código(s) do estabelecimento de saúde
#'
#' @examples
#' \dontrun{ dados = create_output(year_start=2023,
#'                   month_start=1,
#'                   year_end=2023,
#'                   month_end=3,
#'                   state_abbr="CE",
#'                   county_id="230440",
#'                   health_establishment_id=c("2561492","2481286")
#'                   )}
#' @export
create_output <- function(year_start, month_start,
                          year_end, month_end, state_abbr,
                          county_id="all",
                          health_establishment_id="all") {

  tempo_inicio <- system.time({
    state_abbr = toupper(trimws(state_abbr))
    get_counties(state_abbr, county_id)

    download_sigtap_files(
      year_start,
      month_start,
      year_end,
      month_end,
      newer = FALSE)

  outputSIA <- get_datasus(
    year_start,
    month_start,
    year_end,
    month_end,
    state_abbr,
    county_id,
    information_system = "SIA",
    health_establishment_id
  )

  outputSIH_AIH <- get_datasus(
    year_start,
    month_start,
    year_end,
    month_end,
    state_abbr,
    county_id,
    information_system = "SIH-AIH",
    health_establishment_id
  )

  outputSIH_SP <- get_datasus(
    year_start,
    month_start,
    year_end,
    month_end,
    state_abbr,
    county_id,
    information_system = "SIH-SP",
    health_establishment_id
   )

  rm("counties", envir = .GlobalEnv)
  rm("health_establishment", envir = .GlobalEnv)

  # Salva os data frames em arquivos CSV no diretorio atual
  write.csv2(outputSIA, "./data-raw/outputSIA.csv", na="", row.names=FALSE)
  write.csv2(outputSIH_AIH, "./data-raw/outputSIH_AIH.csv", na="", row.names=FALSE)
  write.csv2(outputSIH_SP, "./data-raw/outputSIH_SP.csv", na="", row.names=FALSE)
  })
  cat("Tempo de execução:", tempo_inicio[3]/60, "minutos\n")
  return(list(outputSIA, outputSIH_AIH, outputSIH_SP))

}

#' Create SUS-SIA/SIH database from local DBC files
#'
#' @description
#' [create_output_from_local] preprocess DBC files from SIA and SIH information systems (DATASUS)
#' and match with CNES and SIGTAP information.
#'

#' @param dbc_dir_path Diretório que contêm os arquivos DBC
#' @param health_establishment_id Código(s) do estabelecimento de saúde
#'
#' @examples
#' \dontrun{create_output(tempdir(),
#'                        health_establishment_id=c("2561492","2481286"),
#'                        county_id="230440",
#'                        )}


#' @export
create_output_from_local <- function(dbc_dir_path, health_establishment_id="all", county_id="all") {
  outputSIA <- get_datasus_from_local(
    dbc_dir_path,
    information_system = "SIA",
    county_id,
    health_establishment_id
  )

  outputSIH_AIH <- get_datasus_from_local(
    dbc_dir_path,
    information_system = "SIH-AIH",
    county_id,
    health_establishment_id
  )

  outputSIH_SP <- get_datasus_from_local(
    dbc_dir_path,
    information_system = "SIH-SP",
    county_id,
    health_establishment_id
  )

  write.csv2(outputSIA, "outputSIA.csv", na="", row.names=FALSE)
  write.csv2(outputSIH_AIH, "outputSIH_AIH.csv", na="", row.names=FALSE)
  write.csv2(outputSIH_AIH, "outputSIH_SP.csv", na="", row.names=FALSE)

  return(list(outputSIA, outputSIH_AIH, outputSIH_SP))
}





