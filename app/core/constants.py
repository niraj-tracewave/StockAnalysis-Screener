quarterly_result = {
  "headers": [
    "Sep 2022","Dec 2022","Mar 2023","Jun 2023","Sep 2023","Dec 2023",
    "Mar 2024","Jun 2024","Sep 2024","Dec 2024","Mar 2025","Jun 2025","Sep 2025"
  ],
  "rows": [
    {
      "key": "sales",
      "label": "Sales",
      "type": "group",
      "unit": "Rs Cr",
      "values": [107.12,56.13,73.03,100.58,111.36,107.00,116.62,132.81,154.77,183.55,157.08,183.66,159.68],
      "children": [
        {
          "key": "sales_yoy",
          "label": "YOY Sales Growth %",
          "type": "single",
          "unit": "percentage",
          "values": [-16.38,-61.59,-42.21,-13.62,3.96,90.63,59.69,32.04,38.98,71.54,34.69,38.29,3.17]
        }
      ]
    },

    {
      "key": "expenses",
      "label": "Expenses",
      "type": "group",
      "unit": "Rs Cr",
      "values": [96.39,50.58,78.47,90.77,100.88,97.65,118.01,123.29,147.23,174.56,152.60,167.57,148.54],
      "children": [
        {
          "key": "material_cost",
          "label": "Material Cost %",
          "type": "single",
          "unit": "percentage",
          "values": [63,44,75,63,60,60,70,62,64,67,60,61,56]
        },
        {
          "key": "employee_cost",
          "label": "Employee Cost %",
          "type": "single",
          "unit": "percentage",
          "values": [4,7,7,4,4,4,5,4,4,3,5,3,4]
        }
      ]
    },

    {
      "key": "operating_profit",
      "label": "Operating Profit",
      "type": "group",
      "unit": "Rs Cr",
      "values": [10.73,5.55,-5.44,9.81,10.48,9.35,-1.39,9.52,7.54,8.99,4.48,16.09,11.14],
      "children": [
        {
          "key": "opm",
          "label": "OPM %",
          "type": "single",
          "unit": "percentage",
          "values": [10.02,9.89,-7.45,9.75,9.41,8.74,-1.19,7.17,4.87,4.90,2.85,8.76,6.98]
        }
      ]
    },

    {
      "key": "other_income",
      "label": "Other Income",
      "type": "single",
      "unit": "Rs Cr",
      "values": [4.23,1.10,0.66,1.07,2.27,1.70,2.05,1.93,1.80,2.94,4.43,1.48,5.84]
    },

    {
      "key": "interest",
      "label": "Interest",
      "type": "single",
      "unit": "Rs Cr",
      "values": [3.06,2.91,3.56,3.06,3.74,3.85,4.68,4.12,5.02,6.93,5.63,6.42,7.07]
    },

    {
      "key": "depreciation",
      "label": "Depreciation",
      "type": "single",
      "unit": "Rs Cr",
      "values": [2.63,2.59,3.05,2.98,3.03,3.04,3.04,3.03,3.08,3.08,3.23,3.48,3.07]
    },

    {
      "key": "pbt",
      "label": "Profit Before Tax",
      "type": "single",
      "unit": "Rs Cr",
      "values": [9.27,1.15,-11.39,4.84,5.98,4.16,-7.06,4.30,1.24,1.92,0.05,7.67,6.84]
    },

    {
      "key": "tax",
      "label": "Tax %",
      "type": "single",
      "unit": "percentage",
      "values": [27.18,63.48,-18.96,32.23,25.25,29.33,-12.32,29.30,58.87,-1.56,2160.00,24.90,46.35]
    },

    {
      "key": "net_profit",
      "label": "Net Profit",
      "type": "group",
      "unit": "Rs Cr",
      "values": [6.75,0.41,-9.23,3.28,4.48,2.94,-6.19,3.05,0.51,1.95,-1.03,5.76,3.67],
      "children": [
        {
          "key": "profit_excel_excap",
          "label": "Profit excl Excep",
          "unit": "Rs Cr",
            "type": "single",
          "values": [-14,-91,-50,-63,-34,617,33,-7,-89,-34,83,89,620]
        }
      ]
    },

    {
      "key": "eps",
      "label": "EPS",
      "type": "single",
      "unit": "Rs Cr",
      "values": [1.00,0.06,-1.37,0.49,0.66,0.44,-0.92,0.46,0.08,0.29,-0.15,0.86,0.55]
    }
  ]
}


profit_loss = {
  "headers": [
    "Mar 2014","Mar 2015","Mar 2016","Mar 2017","Mar 2018","Mar 2019",
    "Mar 2020","Mar 2021","Mar 2022","Mar 2023","Mar 2024","Mar 2025","TTM"
  ],
  "rows": [
    {
      "key": "sales",
      "label": "Sales",
      "type": "group",
      "unit": "Rs Cr",
      "values": [230,278,335,489,613,601,604,473,491,353,436,628,684],
      "children": [
        {
          "key": "sales_growth",
          "label": "Sales Growth %",
          "type": "single",
"unit": "Rs Cr",
          "values": [20.74,20.45,46.16,25.45,-2.07,0.59,-21.65,3.72,-28.18,23.49,44.23,None]
        }
      ]
    },

    {
      "key": "expenses",
      "label": "Expenses",
      "type": "group",
      "unit": "Rs Cr",
      "values": [207,251,314,460,566,542,564,448,475,333,407,598,643],
      "children": [
        {
          "key": "material_cost",
          "label": "Material Cost %",
"unit": "percentage",
"type": "single",
          "values": [69,72,75,77,75,70,74,72,68,63,63,63,None]
        },
        {
          "key": "manufacturing_cost",
          "label": "Manufacturing Cost %",
          "unit": "percentage",
"type": "single",
          "values": [15,12,12,14,14,10,9,10,11,12,16,15,None]
        },
        {
          "key": "employee_cost",
          "label": "Employee Cost %",
          "unit": "percentage",
"type": "single",
          "values": [2,2,2,2,2,3,2,3,3,5,4,4,None]
        },
        {
          "key": "other_cost",
          "label": "Other Cost %",
          "unit": "percentage",
"type": "single",
          "values": [4,5,5,1,1,8,8,9,14,14,10,13,None]
        }
      ]
    },

    {
      "key": "operating_profit",
      "label": "Operating Profit",
      "type": "group",
      "unit": "Rs Cr",
      "values": [23,27,20,29,48,59,40,26,16,20,28,31,41],
      "children": [
        {
          "key": "opm",
          "label": "OPM %",
          "unit": "percentage",
          "type": "single",
          "values": [10,10,6,6,8,10,7,5,3,6,6,5,6]
        }
      ]
    },

    {
      "key": "other_income",
      "label": "Other Income",
      "type": "group",
      "unit": "Rs Cr",
      "values": [0,1,1,1,4,16,15,8,13,12,7,11,15],
      "children": [
        {
          "key": "exceptional_items",
          "label": "Exceptional Items",
          "type": "single",
            "unit": "Rs Cr",
          "values": [0,-0.05,-0.02,-1.92,-0.15,0,0.01,-0.01,0.22,0.17,0,0.05,None]
        },
        {
          "key": "other_income_normal",
          "label": "Other Income Normal",
          "type": "single",
            "unit": "Rs Cr",
          "values": [0.31,0.86,1.12,2.53,3.82,16.15,14.80,8.46,13.07,11.37,7.09,11.06,None]
        }
      ]
    },

    {
      "key": "interest",
      "label": "Interest",
      "type": "single",
      "unit": "Rs Cr",
      "values": [5,10,8,9,10,10,7,4,6,11,15,22,26]
    },

    {
      "key": "depreciation",
      "label": "Depreciation",
      "type": "single",
      "unit": "Rs Cr",
      "values": [2,3,3,3,3,3,3,4,4,9,12,12,13]
    },

    {
      "key": "pbt",
      "label": "Profit Before Tax",
      "type": "single",
      "unit": "Rs Cr",
      "values": [17,15,11,18,38,62,45,26,19,11,8,8,16]
    },

    {
      "key": "tax",
      "label": "Tax %",
      "type": "single",
"unit": "percentage",
      "values": [34,36,40,46,37,38,24,30,30,38,43,40,None]
    },

    {
      "key": "net_profit",
      "label": "Net Profit",
      "type": "group",
      "unit": "Rs Cr",
      "values": [11,10,7,10,24,38,34,18,14,7,5,4,10],
      "children": [
        {
          "key": "exceptional_items_at",
          "label": "Exceptional Items AT",
          "type": "single",
"unit": "Rs Cr",
          "values": [0,-0.03,-0.01,-1.03,-0.10,0,0,-0.01,0.15,0.11,0,0.03,None]
        },
        {
          "key": "profit_excl_exceptional",
          "label": "Profit Excl Exceptional",
            "type": "single",
"unit": "Rs Cr",
          "values": [10.89,9.85,6.85,10.73,23.99,37.84,34.08,18.42,13.40,6.59,4.52,4.45,None]
        },
        {
          "key": "profit_for_pe",
          "label": "Profit for PE",
          "type": "single",
"unit": "Rs Cr",
          "values": [10.89,9.85,6.85,10.73,23.99,37.84,34.08,18.42,13.40,6.59,4.52,4.45,None]
        },
        {
          "key": "profit_for_eps",
          "label": "Profit for EPS",
          "type": "single",
"unit": "Rs Cr",
          "values": [10.89,9.82,6.84,9.70,23.89,37.84,34.08,18.41,13.55,6.70,4.52,4.48,None]
        }
      ]
    },

    {
      "key": "eps",
      "label": "EPS",
      "type": "single",
      "unit": "Rs Cr",
      "values": [1.82,1.64,1.16,1.64,4.04,6.38,5.75,2.96,2.00,0.99,0.67,0.67,1.55]
    },

    {
      "key": "dividend_payout",
      "label": "Dividend Payout %",
      "unit": "percentage",
      "type": "single",
      "values": [0,2,6,4,2,4,4,17,17,26,36,33,None]
    }
  ]
}

balance_sheet = {
  "headers": [
    "Mar 2014","Mar 2015","Mar 2016","Mar 2017","Mar 2018","Mar 2019",
    "Mar 2020","Mar 2021","Mar 2022","Mar 2023","Mar 2024","Mar 2025","Sep 2025"
  ],
  "rows": [
    {
      "key": "equity_capital",
      "label": "Equity Capital",
      "type": "single",
      "unit": "Rs Cr",
      "values": [1,1,3,3,3,10,10,11,12,13,13,13,13]
    },
    {
      "key": "reserves",
      "label": "Reserves",
      "type": "single",
      "unit": "Rs Cr",
      "values": [25,35,40,50,74,103,133,165,193,232,246,249,258]
    },

    {
      "key": "borrowings",
      "label": "Borrowings",
      "type": "group",
      "unit": "Rs Cr",
      "values": [51,77,64,77,92,128,112,138,173,176,333,411,417],
      "children": [
        {
          "key": "long_term_borrowings",
          "label": "Long Term Borrowings",
          "type": "single",
          "unit": "Rs Cr",
          "values": [4,1,0,1,2,0,0,15,25,26,116,100,92]
        },
        {
          "key": "short_term_borrowings",
          "label": "Short Term Borrowings",
          "type": "single",
"unit": "Rs Cr",
          "values": [45,75,64,75,89,127,112,123,147,148,215,310,323]
        },
        {
          "key": "lease_liabilities",
          "label": "Lease Liabilities",
          "type": "single",
"unit": "Rs Cr",
          "values": [0,0,0,0,0,0,0,1,1,1,1,1,1]
        },
        {
          "key": "other_borrowings",
          "label": "Other Borrowings",
          "type": "single",
"unit": "Rs Cr",
          "values": [2,1,0,1,1,1,0,0,0,0,0,0,0]
        }
      ]
    },

    {
      "key": "other_liabilities",
      "label": "Other Liabilities",
      "type": "group",
      "unit": "Rs Cr",
      "values": [23,22,30,56,66,49,25,12,20,23,38,77,112],
      "children": [
        {
          "key": "trade_payables",
          "label": "Trade Payables",
          "type": "single",
"unit": "Rs Cr",
          "values": [11,8,16,38,43,34,15,4,6,6,7,38,70]
        },
        {
          "key": "advance_from_customers",
          "label": "Advance from Customers",
          "type": "single",
"unit": "Rs Cr",
          "values": [0,0,0,0,0,1,0,1,0,0,0,0,0]
        },
        {
          "key": "other_liability_items",
          "label": "Other Liability Items",
          "type": "single",
"unit": "Rs Cr",
          "values": [12,14,14,18,23,15,10,7,15,17,31,39,42]
        }
      ]
    },

    {
      "key": "total_liabilities",
      "label": "Total Liabilities",
      "type": "single",
      "unit": "Rs Cr",
      "values": [100,135,137,186,234,291,281,326,398,443,630,750,800]
    },

    {
      "key": "fixed_assets",
      "label": "Fixed Assets",
      "type": "group",
      "unit": "Rs Cr",
      "values": [32,34,35,32,45,44,50,62,69,181,178,179,329],
      "children": [
        { "key":"land","label":"Land","type":"single","unit": "Rs Cr","values":[3.47,3.90,3.90,3.55,9.41,2.75,5.88,11.73,16.37,24.41,24.26,25.30,None] },
        { "key":"building","label":"Building","type":"single","unit": "Rs Cr","values":[13.85,14.05,15.97,15.26,16.75,17.63,19.28,20.40,22.01,71.86,73.26,76.93,None] },
        { "key":"plant","label":"Plant & Machinery","type":"single","unit": "Rs Cr","values":[16.53,18.04,19.55,16.40,20.80,21.06,24.18,29.20,31.78,92.54,97.55,104.13,None] }
      ]
    },

    {
      "key": "other_assets",
      "label": "Other Assets",
      "type": "group",
      "unit": "Rs Cr",
      "values": [67,102,101,153,188,243,227,229,236,244,363,429,460],
      "children": [
        {
          "key": "inventories",
          "label": "Inventories",
          "type": "single",
"unit": "Rs Cr",
          "values": [30,45,43,62,79,115,103,101,109,116,166,240,286]
        },
        {
          "key": "trade_receivables",
          "label": "Trade Receivables",
          "type": "single",
"unit": "Rs Cr",
          "values": [16,31,28,41,51,69,38,29,40,26,58,59,48]
        },
        {
          "key": "cash",
          "label": "Cash & Equivalents",
          "type": "single",
"unit": "Rs Cr",
          "values": [3,1,1,8,17,29,46,49,28,35,43,51,48]
        },
        {
          "key": "loans_advances",
          "label": "Loans & Advances",
          "type": "single",
"unit": "Rs Cr",
          "values": [0,0,0,2,0,1,5,6,10,13,9,13,13]
        },
        {
          "key": "other_assets_items",
          "label": "Other Asset Items",
          "type": "single",
"unit": "Rs Cr",
          "values": [19,25,28,40,40,30,35,45,50,53,86,66,65]
        }
      ]
    },

    {
      "key": "total_assets",
      "label": "Total Assets",
      "type": "single",
      "unit": "Rs Cr",
      "values": [100,135,137,186,234,291,281,326,398,443,630,750,800]
    }
  ]
}


cash_flow = {
  "headers": [
    "Mar 2014","Mar 2015","Mar 2016","Mar 2017","Mar 2018","Mar 2019",
    "Mar 2020","Mar 2021","Mar 2022","Mar 2023","Mar 2024","Mar 2025"
  ],
  "rows": [
    {
      "key": "operating_cash",
      "label": "Cash from Operating Activity",
      "type": "group",
      "unit": "Rs Cr",
      "values": [-12,-15,23,2,18,26,50,9,1,27,-62,5],
      "children": [
        { "key":"profit_from_ops","label":"Profit from operations","type":"single", "unit": "Rs Cr","values":[23,27,20,30,52,72,53,29,27,29,33,37] },
        { "key":"receivables","label":"Receivables","type":"single","unit": "Rs Cr","values":[-7,-15,2,-13,-10,19,31,9,-18,9,-59,6] },
        { "key":"inventory","label":"Inventory","type":"single","unit": "Rs Cr","values":[-19,-15,2,-19,-17,-36,11,2,-8,-7,-50,-73] },
        { "key":"payables","label":"Payables","type":"single","unit": "Rs Cr","values":[1,-2,8,20,4,0,-20,-11,6,1,14,37] },
        { "key":"loans_advances","label":"Loans & Advances","type":"single","unit": "Rs Cr","values":[-2,-3,2,0,0,0,0,0,0,0,0,0] },
        { "key":"other_wc","label":"Other WC items","type":"single","unit": "Rs Cr","values":[-3,-3,-6,-9,1,-14,-11,-13,0,0,0,0] },
        { "key":"wc_changes","label":"Working Capital Changes","type":"single","unit": "Rs Cr","values":[-30,-37,8,-21,-21,-32,11,-13,-20,3,-96,-30] },
        { "key":"direct_tax","label":"Direct Taxes","type":"single","unit": "Rs Cr","values":[-5,-5,-5,-8,-13,-14,-14,-8,-6,-4,1,-2] },
        { "key":"other_op","label":"Other Operating Items","type":"single","unit": "Rs Cr","values":[0,0,0,0,0,0,0,0,0,0,0,0] }
      ]
    },

    {
      "key": "investing_cash",
      "label": "Cash from Investing Activity",
      "type": "group",
      "unit": "Rs Cr",
      "values": [-5,-3,-3,0,-10,-3,-3,-40,-56,-41,-81,-54],
      "children": [
        { "key":"fa_purchased","label":"Fixed Assets Purchased","type":"single","unit": "Rs Cr","values":[-6,-5,-4,-3,-16,-6,-5,-13,-6,-123,-13,-4] },
        { "key":"fa_sold","label":"Fixed Assets Sold","type":"single","unit": "Rs Cr","values":[0,0,0,0,0,0,0,0,0,1,0,0] },
        { "key":"capital_wip","label":"Capital WIP","type":"single","unit": "Rs Cr","values":[0,0,0,0,0,0,0,-30,-59,75,-70,-53] },
        { "key":"investments_sold","label":"Investments Sold","type":"single","unit": "Rs Cr","values":[0,0,0,1,4,0,0,0,1,0,0,0] },
        { "key":"subsidy","label":"Subsidy Received","type":"single","unit": "Rs Cr","values":[0,1,0,0,0,0,0,0,0,0,0,0] },
        { "key":"interest_received","label":"Interest Received","type":"single","unit": "Rs Cr","values":[0,0,0,1,2,2,2,3,2,1,2,3] },
        { "key":"invest_sub","label":"Invest in Subsidiaries","type":"single","unit": "Rs Cr","values":[0,0,-1,0,0,0,0,0,0,0,0,0] },
        { "key":"other_inv","label":"Other Investing Items","type":"single","unit": "Rs Cr","values":[0,1,1,0,0,1,0,0,6,4,0,1] }
      ]
    },

    {
      "key": "financing_cash",
      "label": "Cash from Financing Activity",
      "type": "group",
      "unit": "Rs Cr",
      "values": [18,17,-20,5,2,-13,-30,34,35,21,152,58],
      "children": [
        { "key":"share_proceeds","label":"Proceeds from Shares","type":"single","unit": "Rs Cr","values":[0,0,0,0,0,0,0,10,13,31,11,0] },
        { "key":"borrowings","label":"Proceeds from Borrowings","type":"single","unit": "Rs Cr","values":[0,0,-1,14,14,0,0,26,34,3,157,95] },
        { "key":"repayment","label":"Repayment of Borrowings","type":"single","unit": "Rs Cr","values":[-1,-3,0,0,-1,-3,-17,0,0,0,0,-17] }
      ]
    }
  ]
}


ratios = {
  "headers": [
    "Mar 2014","Mar 2015","Mar 2016","Mar 2017","Mar 2018","Mar 2019",
    "Mar 2020","Mar 2021","Mar 2022","Mar 2023","Mar 2024","Mar 2025"
  ],
  "rows": [
    {
      "key": "debtor_days",
      "label": "Debtor Days",
      "type": "single",
      "unit": "Rs Cr",
      "values": [25,40,31,31,30,42,23,22,30,27,49,34]
    },
    {
      "key": "inventory_days",
      "label": "Inventory Days",
      "type": "single",
      "unit": "Rs Cr",
      "values": [70,82,63,60,62,100,84,107,119,192,221,220]
    },
    {
      "key": "payable_days",
      "label": "Days Payable",
      "type": "single",
      "unit": "Rs Cr",
      "values": [25,15,24,37,34,29,12,4,6,9,9,35]
    },
    {
      "key": "cash_conversion_cycle",
      "label": "Cash Conversion Cycle",
      "type": "single",
      "unit": "Rs Cr",
      "values": [69,107,70,54,59,112,95,125,142,210,261,220]
    },
    {
      "key": "working_capital_days",
      "label": "Working Capital Days",
      "type": "single",
      "unit": "Rs Cr",
      "values": [-9,-3,-5,1,5,17,16,23,12,11,19,-16]
    },
    {
      "key": "roce",
      "label": "ROCE %",
      "type": "single",
      "unit": "percentage",
      "values": [None,27,17,24,32,35,21,11,7,5,5,5]
    }
  ]
}

share_holding_pattern = {
  "period_type": "quarterly",
  "headers": [
    "Dec 2022","Mar 2023","Jun 2023","Sep 2023","Dec 2023","Mar 2024",
    "Jun 2024","Sep 2024","Dec 2024","Mar 2025","Jun 2025","Sep 2025"
  ],
  "rows": [
    {
      "key": "promoters",
      "label": "Promoters",
      "type": "group",
      "unit": "percentage",
      "values": [41.63,41.63,41.64,41.64,41.86,41.86,41.86,41.94,42.22,42.22,42.22,42.23],
      "children": [
        {
          "key": "thottoli_valsaraj",
          "label": "Thottoli Valsaraj",
          "type": "single",
          "unit": "percentage",
          "values": [11.84,11.84,11.85,11.85,12.07,12.07,12.07,12.09,12.17,12.17,12.17,12.17]
        },
        {
          "key": "kambhampati_hari_babu",
          "label": "Kambhampati Hari Babu",
          "type": "single",
          "unit": "percentage",
          "values": [6.76,2.21,6.76,6.76,6.76,6.76,6.76,6.77,6.82,6.82,6.82,6.82]
        },
        {
          "key": "tvr_estates",
          "label": "TVR Estates & Resorts Pvt Ltd",
          "type": "single",
          "unit": "percentage",
          "values": [5.87,5.87,5.87,5.87,5.87,5.87,5.87,5.88,5.92,5.92,5.92,5.92]
        },
        {
          "key": "valsaraj_vijeta",
          "label": "Valsaraj Vijeta",
          "type": "single",
          "unit": "percentage",
          "values": [3.72,3.72,3.72,3.72,3.72,3.72,3.72,3.72,3.75,3.75,3.75,3.75]
        },
        {
          "key": "vinesha_valsaraj",
          "label": "Vinesha Valsaraj",
          "type": "single",
          "unit": "percentage",
          "values": [3.72,3.72,3.72,3.72,3.72,3.72,3.72,3.72,3.75,3.75,3.75,3.75]
        }
      ]
    },

    {
      "key": "fiis",
      "label": "FIIs",
      "unit": "percentage",
      "type": "single",
      "values": [2.13,2.06,1.93,1.93,2.00,1.87,1.87,1.40,0.94,0.94,0.98,1.01]
    },

    {
      "key": "public",
      "label": "Public",
      "type": "group",
      "unit": "percentage",
      "values": [56.24,56.32,56.44,56.41,56.13,56.27,56.26,56.66,56.85,56.85,56.80,56.76],
      "children": [
        {
          "key": "ganta_lakshmi_anusha",
          "label": "Ganta Lakshmi Anusha",
          "unit": "percentage",
          "type": "single",
          "values": [1.65,1.65,1.65,1.65,1.62,1.60,1.57,1.56,1.54,1.54,1.54,1.54]
        }
      ]
    }
  ]
}

EXCHANGE_TYPE_MAP = {
    1: "nse_cm",   # NSE Cash
    2: "nse_fo",   # NSE F&O
    3: "bse_cm",   # BSE Cash
    4: "bse_fo",   # BSE F&O
    5: "mcx_fo",   # MCX Commodities
    7: "ncx_fo",   # NCDEX
    13: "cde_fo",  # CDS (Currency Derivatives)
}


YEAR_OR_MONTY_TO_DAYS_MAP = {
  "1M": 1,
  "6M": 6,
  "1Y": 1,
  "1W": 1,
  "5Y": 5,
  "10Y": 10,
  "15Y": 15,
  "20Y": 20,
  "25Y": 25,
  "30Y": 30,
}

PARENT_CHILD_MAP = {
    "interest earned": [
        "interest or discount on advances or bills",
        "income on investments",
        "interest on balances with reserve bank of India and other inter bank funds",
        "others",
        "total interest earned"
    ],

    "interest expenses": [],
    "operating expenses": [
        "employees cost",
        "details of other operating expenses",
        "description of other operating expenses",
        "claims and benefits paid and other expenses pertaining to insurance business",
        "other operating expenses",
        "total other operating Expenses",
        "total Operating Expenses",
    ],
    "Total expenditure excluding provisions and contingencies": [],
    "Operating profit before provision and contingencies": [],
    "Provisions other than tax and contingencies": [],
    "Total profit (loss) from ordinary activities before tax": [],
    "Provision for Tax": [],
    "Net profit (loss) from ordinary activities after tax": [],
    "Extraordinary items net of tax expenses": [],
    "Net profit (loss) for the period": [],
    "Share of profit (loss) of associates": [],
    "Profit (loss) of minority interest": [],
    "Net Profit (loss) after taxes minority interest and share of profit (loss) of associates": [],
    "Details of equity share capital [Abstract]": [
      "Paid-up equity share capital",
      "Face value of equity share capital"
    ],
    "Reserve excluding revaluation reserves (as per balance sheet of previous accounting year)": [],
    "Analytical ratios": [
        "percentage of share held by government of India",
        "capital adequacy ratio",
        "CET 1 ratio",
        "Additional Tier 1 ratio"
    ],
    "Earnings per share before extraordinary items": [
        "Basic earnings per share before extraordinary items",
        "Diluted earnings per share before extraordinary items",
    ],

    "Earnings per share after extraordinary items": [
        "Basic earnings per share after extraordinary items",
        "Diluted earnings per share after extraordinary items"
    ],

    "NPA Ratios": [
        "Amount of gross non-performing assets",
        "Amount of net non-performing assets",
        "% of gross NPAs",
        "% of net NPAs",
        "Return on assets",
    ],

    "income": [
        "revenue from operations",
        "interest income",
        "dividend income",
        "rental income",
        "fees and commission income",
        "net gain on fair value changes",
        "net gain on derecognition of financial instruments under amortised cost category",
        "sale of products (including excise duty)",
        "sale of services",
        "other revenue from operations",
        "total other revenue from operations",
        "total revenue from operations",
        "other income",
        "total income"
    ],

    "expenses": [
        "cost of materials consumed",
        "purchases of stock-in-trade",
        "changes in inventories of finished goods, work-in-progress and stock-in-trade",
        "employee benefit expense",
        "finance costs",
        "depreciation, depletion and amortisation expense",
        "fees and commission expense",
        "net loss on fair value changes",
        "net loss on derecognition of financial instruments under amortised cost category",
        "impairment on financial instruments",
        "other expenses",
        "total other expenses",
        "total expenses"
    ],

    "other expenses": [
        "cost of equipment and software licences",
        "other expenses"
    ],

    "Total profit before exceptional items and tax": [],
    "Exceptional items": [],
    "Total profit before tax": [],
    "Tax expense": [],
    "Current tax": [],
    "Deferred tax": [],
    "Total tax expenses": [],
    "Net movement in regulatory deferral account balances related to profit or loss and the related deferred tax movement": [],
    "Net Profit Loss for the period from continuing operations": [],
    "Profit (loss) from discontinued operations before tax": [],
    "Tax expense of discontinued operations": [],
    "Net profit (loss) from discontinued operation after tax": [],
    "Share of profit (loss) of associates and joint ventures accounted for using equity method": [],
    "Total profit (loss) for period": [],
    "Other comprehensive income net of taxes": [],
    "Total Comprehensive Income for the period": [],
    "Total profit or loss, attributable to": [
        "Profit or loss, attributable to owners of parent",
        "Total profit or loss, attributable to non-controlling interests",
    ],
    "Total Comprehensive income for the period attributable to": [
        "Comprehensive income for the period attributable to owners of parent",
        "Total comprehensive income for the period attributable to owners of parent non-controlling interests",
    ],
    "Details of equity share capital": [
        "Paid-up equity share capital",
        "Face value of equity share capital"
    ],
    "Details of debt securities": [],
    "Reserves excluding revaluation reserve": [],
    "Earnings per share": [
        "Earnings per equity share for continuing operations",
        "Earnings per equity share",
        "Earnings per equity share for discontinued operations",
    ],
    "Earnings per equity share for continuing operations": [
        "Basic earnings (loss) per share from continuing operations",
        "Diluted earnings (loss) per share from continuing operations",
    ],
    "Earnings per equity share for discontinued operations": [
        "Basic earnings (loss) per share from discontinued operations",
        "Diluted earnings (loss) per share from discontinued operations"
    ],
    "Earnings per equity share": [
        "Basic earnings (loss) per share from continuing and discontinued operations",
        "Diluted earnings (loss) per share from continuing and discontinued operations"
    ],
    "Debt equity ratio": [],
    "Debt service coverage ratio": [],
    "Interest service coverage ratio": [],
    "Disclosure of notes on financial results": [],

}



PARENT_CHILD_MAP_NBFC_INDAS = {
    "income": [
        "revenue from operations",
        "interest income",
        "dividend income",
        "rental income",
        "fees and commission income",
        "net gain on fair value changes",
        "net gain on derecognition of financial instruments under amortised cost category",
        "sale of products (including excise duty)",
        "sale of services",
        "other revenue from operations",
        "income on derecognised (assigned) loans",
        "other operating income",
        "total other revenue from operations",
        "total revenue from operations",
        "other income",
        "total income"
    ],

    "expenses": [
        "cost of materials consumed",
        "purchases of stock-in-trade",
        "changes in inventories of finished goods, work-in-progress and stock-in-trade",
        "employee benefit expense",
        "finance costs",
        "depreciation, depletion and amortisation expense",
        "fees and commission expense",
        "net loss on fair value changes",
        "net loss on derecognition of financial instruments under amortised cost category",
        "impairment on financial instruments",
        "other expenses",
        "total other expenses",
        "total expenses"
    ],

    "other expenses": [
        "others"
    ],

    "Total profit before exceptional items and tax": [],
    "Exceptional items": [],
    "Total profit before tax": [],
    "Tax expense": [],
    "Current tax": [],
    "Deferred tax": [],
    "Total tax expenses": [],
    "Net Profit Loss for the period from continuing operations": [],
    "Profit (loss) from discontinued operations before tax": [],
    "Tax expense of discontinued operations": [],
    "Net profit (loss) from discontinued operation after tax": [],
    "Share of profit (loss) of associates and joint ventures accounted for using equity method": [],
    "Total profit (loss) for period": [],
    "Other comprehensive income net of taxes": [],
    "Total Comprehensive Income for the period": [],
    "Total profit or loss, attributable to": [
        "Profit or loss, attributable to owners of parent",
        "Total profit or loss, attributable to non-controlling interests",
    ],
    "Total Comprehensive income for the period attributable to": [
        "Comprehensive income for the period attributable to owners of parent",
        "Total comprehensive income for the period attributable to owners of parent non-controlling interests",
    ],

    "Details of equity share capital": [
        "Paid-up equity share capital",
        "Face value of equity share capital"
    ],
    "Reserves excluding revaluation reserve": [],
    "Earnings per share": [
        "Earnings per equity share for continuing operations",
        "Earnings per equity share",
        "Earnings per equity share for discontinued operations",
        "Basic earnings per share",
        "Diluted earnings per share"
    ],
    "Earnings per equity share for continuing operations": [
        "Basic earnings per share from continuing operations",
        "Diluted earnings per share from continuing operations",
    ],
    "Earnings per equity share for discontinued operations": [
        "Basic earnings per share from discontinued operations",
        "Diluted earnings per share from discontinued operations"
    ],
    "Debt equity ratio": [],
    "Debt service coverage ratio": [],
    "Interest service coverage ratio": [],
    "Disclosure of notes on financial results": [],

}

PARENT_CHILD_MAP_GI = {
    "Operating income": [
        "Gross Premiums Written",
        "Net Premium written",
        "Premium Earned (Net)",
        "Income from investments (net)",
        "Other income",
        "Other income -Foreign exchange Gain/( Loss)",
        "Total other income",
        "Total income"
    ],

    "Operating expenses": [
        "Commissions & Brokerage (net)",
        "Net commission",
        "Operating Expenses related to insurance business",
        "Employees remuneration and welfare expenses",
        "Other operating expenses",
        "Total other operating expenses",
        "Total operating expenses related to insurance business",
        "Premium Deficiency",
        "Incurred Claims",
        "Claims Paid",
        "Change in Outstanding Claims (incl. IBNR/IBNER)",
        "Total Incurred claims",
        "Total Expense",
        "Underwriting Profit(Loss)",
        "Provisions for doubtful debts (including bad debts written off)",
        "Provisions for diminution in value of investments",
        "Operating Profit/loss:",
        "Appropriations",
        "Transfer to Profit and Loss A/c",
        "Transfer to reserves"
    ],

    "Income in shareholder's account": [
        "Transfer from Policyholders' Fund",
        "Income from investments",
        "Other income",
        "Share of Profit in Associates Companies",
        "Total other income",
        "Total income"
    ],

    "Expenses": [
        "Expenses other than those related to insurance business",
        "Provisions for doubtful debts (including bad debts written off)",
        "Provisions for diminution in value of investments",
        "Total Expense"
    ],

    "Profit / Loss before extraordinary items": [],
    "Extraordinary Items": [],
    "Profit/ (loss) before tax": [],
    "Provision for tax": [],
    "Profit / (loss) after tax": [],
    "Divident per share": [
        "Interim Dividend",
        "Final dividend"
    ],
    "Opening Balance and Appropriations from PAT (Net)": [],
    "Profit (loss) carried to balance sheet": [],
    "Paid up equity capital": [],
    "Reserve and Surplus (Excluding Revaluation Reserve)": [],
    "Fair value change account and revaluation reserve": [],
    "Assets": [
        "Investments",
        "Shareholders Fund",
        "Policyholders' Fund",
        "Total investments",
        "Other Assets (Net of current liabilities and provisions)",
        "Total assets"
    ],

    "Analytical Ratios": [
        "Solvency ratio",
        "Expenses of management ratio",
        "Incurred Claim Ratio",
        "Net retention ratio",
        "Combined ratio",
        "Earning per share",
        "NPA ratios",
        "Yield on Investments",
        "Public shareholding (in case of public sector insurance companies)"
    ],

    "Earning per share": [
        "Basic and diluated EPS before extraordinary items (net of tax expense) for the period (not to be annualized)",
        "Basic and diluted EPS after extraordinary items (net of tax expense) for the period (not to be annualized)"
    ],
    "NPA ratios": [
        "Gross NPAs",
        "Net NPAs",
        "Percentage of Gross NPAs",
        "Percentage of net NPAs"
    ],

    "Yield on Investments": [
        "Without unrealized gains",
        "With unrealised gains"
    ],

    "Public shareholding (in case of public sector insurance companies)": [
        "Number of shares",
        "Percentage of shareholding",
        "Percentage of government holding"
    ],

    "Investor Compliants": [
        "No of investor complaints pending at the beginning of the period",
        "No of investor complaints during the period",
        "No of investor complaints disposed off during the period",
        "No of investor complaints remaining unresolved at the end of the period"
    ]
}