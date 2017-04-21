#!/usr/bin/env python

try:
    import sys
    import pandas as pd
    import matplotlib.pyplot as plt
    import seaborn as sns
except ImportError:
    print("To use this script you have to install some additional libraries.")
    print("Try the following command, and then run this script again")
    print("")
    print("    pip install --user pandas seaborn")
    import sys
    sys.exit(1)

def main():
    path = sys.argv[1]
    data = pd.read_csv(path)
    data['version'].astype(int, inplace=True)
    data['time'].astype(int, inplace=True)
    data['time'] = data['time'] / 1000
    ax = sns.barplot(x='version', y='time', data=data)
    plt.ylabel('time (s)')
    plt.savefig(path + ".pdf")
    
    plt.figure()
    optimized = data[data['version'] > 1]
    ax = sns.barplot(x='version', y='time', data=optimized)
    plt.ylabel('time (s)')
    plt.savefig(path + "-optimized.pdf")

if __name__ == '__main__':
    main()
    
