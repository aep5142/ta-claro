import { redirect } from "next/navigation";

type LegacyOperationsRatePageProps = {
  searchParams: Promise<{
    view?: string;
    start?: string;
    end?: string;
    uf?: string;
  }>;
};

export default async function LegacyOperationsRatePage({
  searchParams,
}: LegacyOperationsRatePageProps) {
  const { view, start, end, uf } = await searchParams;
  const params = new URLSearchParams();
  if (view) {
    params.set("view", view);
  }
  if (start) {
    params.set("start", start);
  }
  if (end) {
    params.set("end", end);
  }
  if (uf) {
    params.set("uf", uf);
  }
  const query = params.toString();
  redirect(`/credit-cards/total-activation-rate${query ? `?${query}` : ""}`);
}
