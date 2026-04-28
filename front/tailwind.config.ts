import type { Config } from "tailwindcss";

const config: Config = {
  content: [
    "./app/**/*.{js,ts,jsx,tsx,mdx}",
    "./components/**/*.{js,ts,jsx,tsx,mdx}",
    "./lib/**/*.{js,ts,jsx,tsx,mdx}",
  ],
  theme: {
    extend: {
      colors: {
        surface: "#08111f",
        panel: "#0f1b2d",
        panelMuted: "#13223a",
        border: "#22344f",
        brand: "#5eead4",
        ink: "#e5eefb",
        muted: "#95a8c7",
      },
    },
  },
  plugins: [],
};

export default config;
