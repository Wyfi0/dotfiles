#
# ~/.bashrc
#

# If not running interactively, don't do anything
[[ $- != *i* ]] && return

# Check if the ntfy.env exists and if so, include it 
if [ -f $HOME/.ntfyenv ]; then
    . $HOME/.ntfyenv
fi

alias nvimk='NVIM_APPNAME="nvim-kickstart" nvim'
# Aliasssss sus [just maps a commant to another]
# Nice colors
# Show hidden files by default as well
alias ls='ls -a --color=auto'
alias grep='grep --color=auto'

# Im sad
alias neofetch='fastfetch'

# Use config command to manage my dotfiles on github from anywhere 
alias config='/usr/bin/git --git-dir=$HOME/.cfg/ --work-tree=$HOME'

alias conftui='gitui --directory $HOME/.cfg/  --workdir $HOME'

alias mountsrv='sudo mount 192.168.2.6:/srv/nfs/md0/ /mnt/Raid'

# List files currently being tracked by config
alias lsconfig='config ls-tree -r HEAD --name-only'

alias smash='~/Downloads/Slippi-Launcher-2.11.6-x86_64.AppImage'

alias pdrop='hugo build && scp -rv public/* 64.227.4.155:/home/raina/docker/nginx/src'
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
	ssh 192.168.2.6 -p 43083
}
# lol
trans() {
	ssh raina@192.168.2.69
}
# lol
drop() {
	ssh raina@64.227.4.155
}
# history awk

alias hawk="history | awk "/$1/""

alias lsawk="$(ls $1) | awk "/$2/""

alias fawk="history | fzf"

alias H="Hyprland"

alias fuck="/usr/bin/pipewire & /usr/bin/pipewire-pulse & /usr/bin/wireplumber"

alias softsh="TERM=xterm-256color ssh soft"

# Finally, introduce yourself, with pokemon!
krabby random 1-6

# holy shit fzf is so fircking cool (this sets up keybinds)
eval "$(fzf --bash)"

# aaaand run starship
eval "$(starship init bash)"

