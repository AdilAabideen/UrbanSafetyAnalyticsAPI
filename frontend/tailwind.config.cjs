/** @type {import('tailwindcss').Config} */
module.exports = {
  content: ["./index.html", "./src/**/*.{js,jsx,ts,tsx}"],
  theme: {
    extend: {
      colors: {
        darkTeal: "#022525",
      },
      fontFamily: {
        sans: ["Hind", "sans-serif"],
      },
    },
  },
  plugins: [],
};

