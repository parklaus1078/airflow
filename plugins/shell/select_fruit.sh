FRUIT=$1
if [ $FRUIT == APPLE ];then
        echo "You selected Apple!"
elif [ $FRUIT == ORANGE ];then
        echo "You selected Orange!"
elif [ $FRUIT == Grape ];then
        echo "You selected Grape!"
else 
        echo "You selected something else!"
fi