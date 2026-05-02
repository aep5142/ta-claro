import { redirect } from "next/navigation";

type LegacyOperationsRatePageProps = {
  searchParams: Promise<{
    view?: string;
  }>;
};

export default async function LegacyOperationsRatePage({
  searchParams,
}: LegacyOperationsRatePageProps) {
  const { view } = await searchParams;
  const query = view ? `?view=${encodeURIComponent(view)}` : "";
  redirect(`/credit-cards/total-activation-rate${query}`);
}
