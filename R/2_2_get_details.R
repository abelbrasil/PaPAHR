
#'  Returns a table with SIGTAP data filtered and organized.
#'
#'@description Esta função filtra os dados baixados pela função `download_sigtap_files` usando o valor especificado no parâmetro `detail_name` e retorna os dados filtrados em um formato estruturado e organizado.
#'
#' @param detail_name String. Nome do arquivo desejado. Existem 8 opcoes possiveis: "Procedimento", "Grupo", "Subgrupo", "Forma de organizacao", "Financiamento", "Rubrica", "CBO", "CID".
#'
#' @return Uma tabela dos dados correpondentes ao parametro `detail_name`
#' @export
#'
get_detail <- function(detail_name) {

  `%>%` <- dplyr::`%>%`
  dir_files <- stringr::str_c(tempdir(), "SIGTAP", sep="\\")

  files_name <- switch(detail_name,
                       "Procedimento" = c("tb_procedimento.txt", "tb_procedimento_layout.txt"),
                       "Grupo" = c("tb_grupo.txt", "tb_grupo_layout.txt"),
                       "Subgrupo" = c("tb_sub_grupo.txt", "tb_sub_grupo_layout.txt"),
                       "Forma de organização" = c("tb_forma_organizacao.txt", "tb_forma_organizacao_layout.txt"),
                       "Financiamento" = c("tb_financiamento.txt", "tb_financiamento_layout.txt"),
                       "Rubrica" = c("tb_rubrica.txt", "tb_rubrica_layout.txt"),
                       "CBO" = c("tb_ocupacao.txt", "tb_ocupacao_layout.txt"),
                       "CID" = c("tb_cid.txt", "tb_cid_layout.txt"))

  raw_detail_file_name <- files_name[1]
  detail_layout_file_name <- files_name[2]

  raw_detail <- list.files(dir_files,full.name=TRUE, recursive=TRUE) %>%
    stringr::str_subset(raw_detail_file_name) %>%
    purrr::map(get_sigtap_file, type="raw") %>%
    dplyr::bind_rows()

  detail_layout <- list.files(dir_files,full.name=TRUE, recursive=TRUE) %>%
    stringr::str_subset(detail_layout_file_name) %>%
    purrr::map(get_sigtap_file, type="layout") %>%
    dplyr::bind_rows() %>%
    tidyr::separate(value, into=c("column_name", "size", "start", "end", "type"), sep=",") %>%
    dplyr::filter(column_name != "Coluna")

  details <- raw_detail %>%
    dplyr::left_join(detail_layout, by="file_version_id") %>%
    dplyr::mutate(detail = purrr::pmap(list(raw_detail, start, end), ~ stringr::str_sub(..1, ..2, ..3))) %>%
    dplyr::select(column_name, file_version_id, detail) %>%
    tidyr::pivot_wider(names_from=column_name, values_from=detail) %>%
    tidyr::unnest(-file_version_id)

  return(details)
}

get_sigtap_file <- function(file_name, type) {
  `%>%` <- dplyr::`%>%`
  loc <- stringr::str_locate(file_name, "SIGTAP")

  file_version_id <- stringr::str_sub(file_name, loc[2] + 2, loc[2] + 7)

  raw_file <- file_name %>% readLines()

  if (type == "raw") {
    raw_file <-  tibble::tibble_row(raw_detail = list(raw_file),
                                    file_version_id = file_version_id)

    return(raw_file)
  }

  if (type == "layout") {
    layout_file <- raw_file %>%
      tibble::as_tibble() %>%
      tibble::add_column(file_version_id)

    return(layout_file)
  }
}
