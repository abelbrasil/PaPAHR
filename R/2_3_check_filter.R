
#' Receives a database and returns a filtered version of it.
#'
#' @description Função auxiliar que filtra uma base de dados com base em um valor específico.
#'
#' @param df um DataFrame que sera filtrado.
#' @param var_value string or a vector of strings. Os valores a serem filtrados. Se o valor for nulo, a função retorna o mesmo dataframe recebido.
#' @param var_name string. Nome da coluna do DataFrame que será utilizada para a filtragem.
#'
#' @return Retorna uma tabela filtrada pelo valor de `var_value` na coluna `var_name`.
#' @export
check_filter <- function(df, var_value, var_name) {

  if(!is.null(var_value)){
    if (length(var_value) == 1) {
      filtered_df <- dplyr::filter(df, .data[[var_name]] == var_value)

      return(filtered_df)
    } else {
      filtered_df <- dplyr::filter(df, .data[[var_name]] %in% var_value)

      return(filtered_df)
    }
  }else{
    return(df)
  }
}


