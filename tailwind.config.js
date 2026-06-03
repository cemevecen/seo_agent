/** @type {import('tailwindcss').Config} */
module.exports = {
  darkMode: "class",
  content: ["./templates/**/*.html"],
  safelist: [
    // /ad — xl grid düzeni (ad.html)
    "xl:grid-cols-12",
    "xl:col-span-3",
    "xl:col-span-9",
    "xl:grid-cols-5",
    // ASC kart renkleri — JS'de dinamik olarak oluşturulduğu için safelist gerekiyor
    {
      pattern:
        /^(text|bg|border)-(sky|cyan|indigo|violet|emerald|fuchsia|amber|teal|rose|slate)-(50|100|200|300|400|500|600|700|800|900|950)(\/\d+)?$/,
      variants: ["dark", "hover", "dark:hover"],
    },
    // Opacity varyantlar (bg-*/70 gibi)
    {
      pattern:
        /^(bg|border)-(sky|cyan|indigo|violet|emerald|fuchsia|amber|teal|rose|slate)-(50|100|200|700|800|900|950)\/(30|40|50|60|70|80)$/,
      variants: ["dark"],
    },
  ],
  theme: {
    extend: {},
  },
  plugins: [],
};
