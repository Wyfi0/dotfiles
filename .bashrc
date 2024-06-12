#
# ~/.bashrc
#

# If not running interactively, don't do anything
[[ $- != *i* ]] && return

# Aliasssss sus [just maps a commant to another]
# Nice colors
# Show hidden files by default as well
alias ls='ls -a --color=auto'
alias grep='grep --color=auto'

# Im sad
alias neofetch='fastfetch'

# Use config command to manage my dotfiles on github from anywhere 
alias config='/usr/bin/git --git-dir=$HOME/.myconf/ --work-tree=$HOME'

alias mountsrv='sudo mount 192.168.0.12:/srv/nfs/md0/ /mnt/Raid'

# List files currently being tracked by config
alias lsconfig='config ls-tree -r HEAD --name-only'

# How much of the history to store in ram in command count
HISTSIZE=10000
# How many commands to save on disk (im a maniac)
HISTFILESIZE=10000

# Sets up the prompt before where you type
PS1='[\u@\h \W]\$ '

# Set neovim as default
export EDITOR='nvim'
export VISUAL='nvim'

# Little cutie macros >w<
today() {
	echo -n "Today's date is: "
	date +"%A, %B %-d, %Y"
}
# Im lazyyyyyy ^-^
hypr() {
	nvim ~/.config/hypr/hyprland.conf
}
# Make a directory and cd into it
mkcd() {
	mkdir -p -- "$1" && cd -P -- "$1"
}
# lol
shh() {
	ssh wyatt@192.168.0.12 -p 43083
}
# history awk

alias hawk="history | awk "/$1/""

alias lsawk="$(ls $1) | awk "/$2/""

# Finally, introduce yourself, with pokemon!
krabby random 1-6

# aaaand run starship
eval "$(starship init bash)"

