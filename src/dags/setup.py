import yaml
import sys

def load_config(environment):
    with open("config.yaml", "r") as file:
        config = yaml.safe_load(file)
    
    if environment in config:
        return config[environment]
    else:
        raise ValueError(f"Environment '{environment}' not found in configuration file")

if __name__ == "__main__":
    if len(sys.argv) != 2:
        print("Usage: python setup.py <environment>")
        sys.exit(1)

    environment = sys.argv[1]
    config = load_config(environment)
    
    # Set variables (this example just prints them)
    for key, value in config.items():
        print(f"{key} = {value}")
    
    # Here you can set the variables to be used throughout your program
    # For example:
    globals().update(config)
