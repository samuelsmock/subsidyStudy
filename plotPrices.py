

import matplotlib.pyplot as plt
import pandas as pd

prices = pd.read_csv('/Users/sunshinedaydream/Desktop/thesis_data_local/non-spatial/codeDictionaries/priceDict.csv')

def plotPrice(priceDict):
    prefix_colors ={}
    color_options = ['b', '#ed8611',  '#179c41', '#c90822', 'g', 'b', ]
    plt.figure(figsize=(4, 6))

    for column in priceDict.columns:
        if column == "Year":
            continue

        prefix = column.split('_')[0]  # Get the prefix of the column name

        #If the prefix is not in the dictionary, assign a new color
        if prefix not in prefix_colors:
             prefix_colors[prefix] = color_options.pop(0)
    
        
        if column.endswith('_Elec'):
            plt.plot(pd.to_datetime(priceDict['Year'], format = '%Y'), priceDict[column], label=prefix + '\nElec-HPT ', color = prefix_colors[prefix], linewidth = 2.5, linestyle = 'solid')
        if column.endswith('_Gas'):
            plt.plot(pd.to_datetime(priceDict['Year'], format = '%Y'), priceDict[column], label=prefix + "\nNG-Piped", color = prefix_colors[prefix], linewidth = 2.5, linestyle = 'dashed')


    plt.title('Representative Price Trajectories Used in Modelling')
    plt.xlabel('Year')  # Replace with an appropriate label
    plt.ylabel('Cost (Euros/kWh)')  # Replace with an appropriate label
    plt.legend(loc='center left', bbox_to_anchor=(1, 0.5))  # Add a legend to differentiate columns
    plt.grid(True)  # Add grid lines

    plt.show()

plotPrice(prices)