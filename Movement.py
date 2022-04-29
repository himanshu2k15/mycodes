import pyautogui,time
num_sec=10
x,y=pyautogui.size()
for i in range(10000):
    time.sleep(5) 
    pyautogui.moveTo(1,1,duration=num_sec)
    pyautogui.moveTo(x-1,1,duration=num_sec)
    pyautogui.moveTo(x-10,y-10,duration=num_sec)
    pyautogui.moveTo(1,y-1,duration=num_sec)
    pyautogui.moveTo(1,1,duration=num_sec)
    time.sleep(5)
    print('Moving Frame - ',i)
