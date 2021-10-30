using System;

namespace APIAcoes.Data
{
    public class HistoricoAcao
    {
        public int Id { get; set; }
        public string HistLancamento { get; set; }
        public string Codigo { get; set; }
        public double Valor { get; set; }
        public DateTime DataReferencia { get; set; }
        public string CodCorretora { get; set; }
        public string NomeCorretora { get; set; }
    }
}