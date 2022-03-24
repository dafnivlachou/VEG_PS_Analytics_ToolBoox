#!/usr/bin/env python
""" StatTools.py: A collection of classes utilized in statistical analysis. """

__author__ = "Chris Jacobs"
__version__ = "0.1"
__email__ = "chris.jacobs@bayer.com"

import matplotlib.pyplot as plt
import statsmodels.stats.diagnostic as ssd
import scipy.stats as stats
import statistics


class LmAssumptions:
    """
    While linear regression is a pretty simple task, there are several assumptions for the model that we may want to
    validate, namely LINE in Python:

    - Linearity
    - Independence
    - Normality
    - Equal variance (or homoscedasticity)

    The good news is that some of the model conditions are more forgiving than others. So, we really need to learn when
    we should worry the most and when it's okay to be more carefree about model violations. Here's a pretty good summary
    of the situation:

    - All tests and intervals are very sensitive to even minor departures from independence.
    - All tests and intervals are sensitive to moderate departures from equal variance.
    - The hypothesis tests and confidence intervals for β0 and β1 are fairly "robust" (that is, forgiving) against
      departures from normality.
    - Prediction intervals are quite sensitive to departures from normality.

    Source: https://online.stat.psu.edu/stat462/node/116/

    Create the LmAssumptions object by running:

     ``lma = LmAssumptions(model, y)``

    Next, run the apply to obtain the figure and statistics:

     ``fig, results_list = lma.apply()``

    The DataAccess.WriteData class can be used to save the generated output:

     ``from DataAccess import WriteData``

     ``wd = WriteData()``

     ``wd.saveplot(fig, "C:/plot.png")``

     ``wd.list_to_textfile(results_list, "C:/results.txt")``

    .. image:: /images/Lm_plot.png
      :width: 750

    :param model: a Statsmodels OLS fitted model object
    :param y: the dependent variable as a numpy array / pandas column or python list

    """

    def __init__(self, model, y):
        self.model = model
        self.y = y

    def apply(self):
        """
        Run the analytics for the assumptions of a linear regression.

        :return: a matplotlib figure object with the plots and a results list with the test output.
        """
        results_list = []
        fig, (ax1, ax2) = plt.subplots(1, 2, figsize=(12, 6))
        # Plot a standardized residuals plot to check for linearity and equal variance
        # Also check for outliers (More than 2 standard deviations warrent investigation):
        self._plot_standardized_residuals(ax1)
        # QQ plot
        self._plot_qq(ax2)
        # Test for linearity, doesn't seem to work when using squared terms:
        pred_val = self.model.fittedvalues
        residual = self.y - pred_val
        # Test for equal variance / homoscedasticity using Breusch-Pagan test:
        labels = ['LM Statistic', 'LM-Test p-value', 'F-Statistic', 'F-Test p-value']
        try:
            white_test = ssd.het_white(residual, self.model.model.exog)
        except ValueError:
            white_test = None
        try:
            bp_test = ssd.het_breuschpagan(residual, self.model.model.exog)
        except ValueError:
            bp_test = None
        if bp_test is not None:
            results_list.append('Breusch-Pagan test for homoscedascity: \n')
            results_list.append(str((dict(zip(labels, bp_test)))))
        else:
            results_list.append('Breusch-Pagan test failed.')
        results_list.append('\n')
        if white_test is not None:
            results_list.append('White test for homoscedascity: \n')
            results_list.append(str(dict(zip(labels, white_test))))
        else:
            results_list.append('White test failed.')
        plt.tight_layout()  # make sure everything is shown properly

        return fig, results_list

    def _plot_standardized_residuals(self, ax):
        """
        This function creates a standardized residuals vs fitted values plot to inspect for problems with
        heteroscedasticity.

        :param ax: Matplotlib axes-object
        :return: Matplotlib axes-object
        """
        pred_val = self.model.fittedvalues
        influence = self.model.get_influence()
        standardized_residuals = influence.resid_studentized_internal
        ax.scatter(pred_val, standardized_residuals, s=1)
        ax.set_xlabel('Fitted Value')
        ax.set_ylabel('Standardized Residuals')
        return ax

    def _plot_qq(self, ax):
        """
        This function creates a QQ-plot (Quantile-Quantile plot) to inspect whether the data follows a normal
        distribution. It uses scipy.

        :param ax: Matplotlib axes-object
        :return: Matplotlib axes-object
        """
        influence = self.model.get_influence()
        standardized_residuals = influence.resid_studentized_internal
        p_shapiro, p_ks = self._test_normality(standardized_residuals)
        stats.probplot(standardized_residuals, dist='norm', plot=ax)
        if p_shapiro is not None:
            ax.text(0.05, 0.90, 'Shapiro p-value = {}'.format(round(p_shapiro, 5)), fontsize=10, transform=ax.transAxes)
        if p_ks is not None:
            ax.text(0.05, 0.85, 'Kolmogorov-Smirnov p-value = {}'.format(round(p_ks, 5)), fontsize=10,
                    transform=ax.transAxes)
        return ax

    @staticmethod
    def _test_normality(var):
        """
        This function tests whether the data in var is taken from a normal distribution using the Shapiro and
        Kolmogorov-Smirnov tests. It uses the mean and standard deviation of the data itself as input for the KS-test.
        The threshold and print_res are optional, if print_res is True, it will print the outcome of the tests to the
        terminal. It will state wether it passed the test depending on the value of threshold.
        If these are not given, this function will return the p-values for the Shapiro and KS test only.

        :param var: data to be tested for normality (numpy array)
        :return: p-value of the Shapiro test, p-value of the KS test.
        """
        try:
            shapiro_res = stats.shapiro(var)
            p_shapiro = shapiro_res[1]
        except ValueError:
            p_shapiro = None

        try:
            average = statistics.mean(var)
            stdeviation = statistics.stdev(var)
            ks_res = stats.kstest(var, 'norm', args=(average, stdeviation))
            p_ks = ks_res[1]
        except ValueError:
            p_ks = None
        return p_shapiro, p_ks
