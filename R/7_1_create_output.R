
#' Create SUS-SIA/SIH database
#'
#' @description Processar arquivos dos sistemas de informação SIA e SIH (DATASUS) e combiná-los com informações do CNES e SIGTAP.
#'
#' @param year_start Um numero de 4 digitos, indicando o ano de inicio para o download dos dados.
#' @param month_start Um numero de 2 digitos, indicando o mes de inicio para o download dos dados.
#' @param year_end Um numero de 4 digitos, indicando o ano de termino para o download dos dados.
#' @param month_end Um numero de 2 digitos, indicando o mes de termino para o download dos dados.
#' @param state_abbr String. Sigla da Unidade Federativa
#' @param county_id Codigo(s) do Municipio de Atendimento
#' @param health_establishment_id COdigo(s) do estabelecimento de saude
#'
#' @examples
#'   dados = create_output(
#'     year_start = 2023,
#'     month_start = 1,
#'     year_end = 2023,
#'     month_end = 3,
#'     state_abbr = "CE",
#'     county_id = "230440",
#'     health_establishment_id = c("2561492", "2481286")
#'   )
#' @export
create_output <-
  function(year_start,
           month_start,
           year_end,
           month_end,
           state_abbr,
           county_id = "all",
           health_establishment_id = "all") {

    tempo_inicio <- system.time({
      state_abbr = toupper(trimws(state_abbr))

      get_counties(state_abbr, county_id)
      get_CNES()


      download_sigtap_files(year_start,
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
      write.csv2(outputSIA,
                 "./data-raw/outputSIA.csv",
                 na = "",
                 row.names = FALSE)
      write.csv2(
        outputSIH_AIH,
        "./data-raw/outputSIH_AIH.csv",
        na = "",
        row.names = FALSE
      )
      write.csv2(
        outputSIH_SP,
        "./data-raw/outputSIH_SP.csv",
        na = "",
        row.names = FALSE
      )
    })
    cat("Tempo de execução:", tempo_inicio[3]/60,"minutos\n")
    return(list(outputSIA, outputSIH_AIH, outputSIH_SP))

  }

