import { getSupabaseBrowserClient } from "@/lib/supabase-client";
import type { OperationName } from "@/lib/credit-card-config";

const METRICS_PAGE_SIZE = 1000;

export type CreditCardMetricRow = {
  operation_type: OperationName;
  dataset_code: string;
  institution_code: string;
  institution_name: string;
  period_month: string;
  transaction_count: string;
  nominal_volume_thousands_millions_clp: string;
  uf_date_used: string;
  uf_value_used: string;
  real_value_uf: string;
  average_ticket_uf: string;
  source_dataset_code: string;
  updated_at: string;
};

export async function fetchDatasetBoundary(
  operation: OperationName,
  boundary: "latest" | "earliest"
): Promise<string | null> {
  const supabase = getSupabaseBrowserClient();
  const ascending = boundary === "earliest";

  const { data, error } = await supabase
    .from("bank_credit_card_ops_metrics")
    .select("period_month")
    .eq("operation_type", operation)
    .order("period_month", { ascending })
    .limit(1);

  if (error) {
    throw new Error(error.message);
  }

  return data?.[0]?.period_month ?? null;
}

export async function fetchLatestUfValue(todayIso: string): Promise<{ ufDate: string; value: number }> {
  const supabase = getSupabaseBrowserClient();
  const { data, error } = await supabase
    .from("uf_values")
    .select("uf_date, value")
    .lte("uf_date", todayIso)
    .order("uf_date", { ascending: false })
    .limit(1);

  if (error) {
    throw new Error(error.message);
  }

  if (!data?.length) {
    throw new Error("No UF value is available up to today's Santiago date.");
  }

  return {
    ufDate: data[0].uf_date,
    value: Number(data[0].value),
  };
}

export async function fetchCreditCardMetrics(
  operation: OperationName,
  startDateIso: string,
  endDateIso: string
): Promise<CreditCardMetricRow[]> {
  const supabase = getSupabaseBrowserClient();
  const rows: CreditCardMetricRow[] = [];
  let pageStart = 0;

  while (true) {
    const pageEnd = pageStart + METRICS_PAGE_SIZE - 1;
    const { data, error } = await supabase
      .from("bank_credit_card_ops_metrics")
      .select(
        "operation_type,dataset_code,institution_code,institution_name,period_month,transaction_count,nominal_volume_thousands_millions_clp,uf_date_used,uf_value_used,real_value_uf,average_ticket_uf,source_dataset_code,updated_at"
      )
      .eq("operation_type", operation)
      .gte("period_month", startDateIso)
      .lte("period_month", endDateIso)
      .order("period_month", { ascending: true })
      .order("institution_name", { ascending: true })
      .range(pageStart, pageEnd);

    if (error) {
      throw new Error(error.message);
    }

    const pageRows = (data ?? []) as CreditCardMetricRow[];
    rows.push(...pageRows);

    if (pageRows.length < METRICS_PAGE_SIZE) {
      break;
    }

    pageStart += METRICS_PAGE_SIZE;
  }

  return rows;
}
