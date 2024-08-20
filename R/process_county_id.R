
#' Removes the last character from county_id if it is 7 characters long.
#'
#' @param county_id string ou vetor de strings. Código do Município de Atendimento.
#'
#' @return Retorna o `county_id` com um caractere a menos se ele tiver 7 caracteres.
#'
#' @export
process_county_id <- function(county_id) {

  if (!is.null(county_id)) {

    county_id <- sapply(county_id, function(id) {
      if (nchar(id) == 7) {
        substr(id, 1, nchar(id) - 1)
      } else {
        id
      }
    })
  }
  return(county_id)
}
