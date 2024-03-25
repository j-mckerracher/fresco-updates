from classes.base_widget_manager import BaseWidgetManager
from classes.widget_state_manager import WidgetStateManager


class NotebookUtilities:
    def __init__(self):
        self.widget_manager = BaseWidgetManager()
        self.widget_state_manager = WidgetStateManager(self.widget_manager)

    def display_statistics_widgets(self):
        self.widget_manager.display_statistics_widgets()

    def display_plots(self):
        self.widget_manager.display_plots()

    def pearson_correlation(self):
        self.widget_manager.pearson_correlation()