Instructions on how to SSH into the Google Compute Engine Servers

1. Download gcloud cli tools using instructions [here](https://cloud.google.com/sdk/docs/install)
1. in your terminal, run `gcloud init` and follow instructions
1. Pick cloud project to use: ex. miurban-datajournalism
1. run `gcloud compute config-ssh` and create ssh key
1. run `gcloud compute os-login ssh-keys add --key-file=[key_file_location]`
- key_file_location can be found by opening in vs code (in the top bar search for `>Remote-SSH: Open SSH Configuration File...` and then choose your config file). Once inside, find the configs under `Google Compute Engine Section`. key_file_location will be what is listed after `IdentityFile` with `.pub` added at the end.
1. Lastly, back in the config file in vs code, under each Host section under `Google Compute Engine Section`, add another parameter called `User` and add `[cnet]_uchicago_edu`

