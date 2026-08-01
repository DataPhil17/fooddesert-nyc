// See https://observablehq.com/framework/config for documentation.
export default {
  title: "NYC Food Desert Analysis",

  pages: [
    { name: "Home",          path: "/" },
    { name: "Methodology",   path: "/methodology" },
    { name: "Findings",      path: "/findings" },
    { name: "Map",           path: "/map" },
    { name: "Intervention",  path: "/intervention" },
  ],

  head: `
    <style>
      /* ── Earth tone / deep red color palette ───────────────── */
      :root {
        --theme-foreground:           #2C2416;
        --theme-background:           #F5F0EB;
        --theme-background-alt:       #EDE6DC;
        --theme-foreground-muted:     #6B5C48;
        --theme-foreground-faint:     #9C8C78;
        --theme-foreground-fainter:   #C4B49A;
        --theme-foreground-faintest:  #DDD4C8;
        --theme-border:               #C4B49A;
        --theme-accent:               #8B2C2C;
      }

      /* Sidebar */
      #observablehq-sidebar {
        background: #1A0F08 !important;
        border-right: 1px solid #3D3020 !important;
      }
      #observablehq-sidebar a {
        color: #F5E6C8 !important;
        font-weight: 500;
      }
      #observablehq-sidebar a:hover {
        color: #FFFFFF !important;
        background: #5C1515 !important;
        border-radius: 4px;
        text-decoration: none;
      }
      #observablehq-sidebar a[aria-current="page"],
      #observablehq-sidebar a[aria-current="true"] {
        color: #1A0F08 !important;
        background: #F5E6C8 !important;
        border-radius: 4px;
        font-weight: 700;
        border-left: 3px solid #8B2C2C !important;
        padding-left: 6px;
      }
      #observablehq-sidebar summary {
        color: #C4B49A !important;
        font-size: 11px;
        text-transform: uppercase;
        letter-spacing: 0.06em;
      }
      /* Title in sidebar */
      #observablehq-sidebar .observablehq-link,
      #observablehq-sidebar h1,
      #observablehq-sidebar h2 {
        color: #F5E6C8 !important;
      }

      /* Header */
      #observablehq-header {
        background: #1A0F08 !important;
        border-bottom: 2px solid #8B2C2C !important;
        color: #F5E6C8 !important;
      }
      #observablehq-header a,
      #observablehq-header span,
      #observablehq-header div {
        color: #F5E6C8 !important;
        font-weight: 600;
      }
      #observablehq-header * {
        color: #F5E6C8 !important;
      }

      /* Main content area */
      #observablehq-main {
        background: #F5F0EB !important;
      }

      /* Cards */
      .observablehq-card {
        background: #EDE6DC !important;
        border: 1px solid #C4B49A !important;
        border-radius: 8px;
      }

      /* Headings */
      h1 { color: #2C2416; border-bottom: 2px solid #8B2C2C; padding-bottom: 8px; }
      h2 { color: #4A3828; }
      h3 { color: #6B5C48; }

      /* Links */
      a { color: #8B2C2C; }
      a:hover { color: #6B1F1F; }

      /* Code blocks */
      pre, code {
        background: #EDE6DC !important;
        border: 1px solid #C4B49A !important;
        border-radius: 4px;
      }

      /* Tip / note callouts */
      .observablehq-tip {
        border-left: 4px solid #8B2C2C !important;
        background: #EDE6DC !important;
      }

      /* Footer */
      #observablehq-footer {
        background: #2C2416 !important;
        color: #9C8C78 !important;
        border-top: 1px solid #3D3020 !important;
        font-size: 12px;
      }
    </style>
  `,

  footer: `
    <div style="text-align:center; padding: 12px 0; color:#9C8C78; font-size:12px;">
      NYC Food Desert Analysis &mdash; Philippe &mdash;
      Data: ACS 2023, USDA FNS, NYC Open Data, NY State Dept of Agriculture &mdash;
      <a href="https://github.com/DataPhil17/fooddesert-nyc"
         style="color:#C4B49A;" target="_blank">GitHub</a>
    </div>
  `,

  root: "src",
  search: true,
  toc: true,
};