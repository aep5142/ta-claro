import { redirect } from "next/navigation";

export default function HomePage() {
  redirect("/credit-cards/purchases?view=volume");
}
