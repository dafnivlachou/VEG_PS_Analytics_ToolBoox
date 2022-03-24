#!/usr/bin/env python
""" Visualization.py: A collection of classes to visualize with data. """

__author__ = "Chris Jacobs"
__version__ = "0.1"
__email__ = "chris.jacobs@bayer.com"

from Logging import Logger
import matplotlib.pyplot as plt


class Plotting:
    """
    This class contains some more frequently used plotting methods.

    """

    def __init__(self, loglevel='INFO'):
        self.logger = Logger('Visualization.Plotting', loglevel).logger

    def scatter_plot(self, x_var, y_var, df, x_label=None, y_label=None, title=None, grouping_var=None, color=None,
                     x_rotation=0, y_rotation=0, labelsize=8):
        """
        This function creates a scatterplot between the x-variable and the y-variable. It can take an optional
        grouping_variable, by which the data points are colored if given. If the x- and y-label are not given, the
        x- and y-variable names are used to label the axis instead. It can only plot data present in a pandas dataframe.

        .. image:: /images/Scatterplot_example.png
          :width: 750

        :param x_var: the x-variable name as given in the dataframe
        :param y_var: the y-variable name as given in the dataframe
        :param df: the pandas dataframe containing the data
        :param x_label: (Optional) a name to label the x-axis
        :param y_label: (Optional) a name to label the y-axis
        :param title: (Optional) a name to label the plot
        :param grouping_var: (Optional) a grouping factor present in the dataframe by which to color the data points.
        :param color: (Optional) provide a color ('green', 'red' etc.) Only works when grouping = None
        :param x_rotation: (Optional) how many degrees the x_labels should be rotated
        :param y_rotation: (Optional) how many degrees the y_labels should be rotated
        :param labelsize: (Optional) how large the x- and y-ticks should be (Default=8)
        :return: a matplotlib figure object with the plot.
        """
        fig, ax = plt.subplots()
        ax.margins(0.05)  # Optional, just adds 5% padding to the autoscaling
        if grouping_var is not None:
            groups = df.groupby(grouping_var)
            for nm, group in groups:
                ax.plot(group[x_var], group[y_var], marker='o', linestyle='', ms=2, label=nm)
            ax.legend()
        else:
            if color is None:
                ax.plot(df[x_var], df[y_var], marker='o', linestyle='', ms=2)
            else:
                ax.plot(df[x_var], df[y_var], marker='o', color=color, linestyle='', ms=2)
        if x_label is not None:
            ax.set_xlabel(x_label)
        else:
            ax.set_xlabel(x_var)
        if y_label is not None:
            ax.set_ylabel(y_label)
        else:
            ax.set_ylabel(y_var)
        if title is not None:
            fig.suptitle(title)
        plt.xticks(rotation=x_rotation, fontsize=labelsize)
        plt.yticks(rotation=y_rotation, fontsize=labelsize)
        plt.tight_layout()
        self.logger.info('Scatterplot created.')
        return fig
