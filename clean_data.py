import pandas as pd
df = pd.read_csv("german_credit_data_original.csv")
df[["id", "age", "job", "saving_accounts", "checking_account", "credit_amount", "duration", "purpose", "risk"]].to_csv("german_credit_data.csv", index=False)